//go:build linux

package cgroup

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const defaultRoot = "/sys/fs/cgroup"

func New(root string) (*Manager, error) {
	if root == "" {
		root = defaultRoot
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return newManagerWithFS(&realFS{root: filepath.Clean(abs)}, managerOptions{
		terminator: pidfdTerminator{},
	}), nil
}

type realFS struct {
	root string
	mu   sync.Mutex
	held *realRoot
}

type realRoot struct {
	fd       int
	identity RootIdentity
}

type realObject struct {
	rootPath string
	leafName string
	rootfd   int
	leaffd   int
	root     ObjectIdentity
	leaf     ObjectIdentity
	closed   bool
}

func (object *realObject) LeafName() string {
	if object == nil {
		return ""
	}
	return object.leafName
}

func (object *realObject) RootObject() ObjectIdentity {
	if object == nil {
		return ObjectIdentity{}
	}
	return object.root
}

func (object *realObject) LeafObject() ObjectIdentity {
	if object == nil {
		return ObjectIdentity{}
	}
	return object.leaf
}

func (object *realObject) Close() error {
	if object == nil || object.closed {
		return nil
	}
	object.closed = true
	var err error
	if object.leaffd >= 0 {
		if closeErr := unix.Close(object.leaffd); closeErr != nil {
			err = closeErr
		}
		object.leaffd = -1
	}
	if object.rootfd >= 0 {
		object.rootfd = -1
	}
	return err
}

func (fs *realFS) RootIdentity(ctx context.Context) (RootIdentity, error) {
	if err := ctx.Err(); err != nil {
		return RootIdentity{}, err
	}
	held, err := fs.heldRoot(ctx)
	if err != nil {
		return RootIdentity{}, err
	}
	return held.identity, nil
}

func (fs *realFS) heldRoot(ctx context.Context) (*realRoot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.held != nil {
		return fs.held, nil
	}
	held, err := fs.openAndLeaseRoot()
	if err != nil {
		return nil, err
	}
	fs.held = held
	return held, nil
}

func (fs *realFS) openAndLeaseRoot() (*realRoot, error) {
	var statfs unix.Statfs_t
	if err := unix.Statfs(fs.root, &statfs); err != nil {
		return nil, fmt.Errorf("%w: cgroupfs absent: %v", ErrUnsupported, err)
	}
	if statfs.Type != unix.CGROUP2_SUPER_MAGIC {
		return nil, fmt.Errorf("%w: cgroup root is not cgroup2", ErrUnsupported)
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return nil, err
	}
	rootObject, err := identityFromFD(rootfd)
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	exclusive, err := fs.establishExclusiveDelegation(rootfd)
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	selfCgroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, fmt.Errorf("%w: read /proc/self/cgroup: %v", ErrUnsupported, err)
	}
	mountData, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, fmt.Errorf("%w: read /proc/self/mountinfo: %v", ErrUnsupported, err)
	}
	mounts, err := parseMountInfo(string(mountData))
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	mount, err := classifyUnifiedCgroupRoot(string(selfCgroup), mounts, fs.root)
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	cgroupTypeData, err := readRootFile(rootfd, "cgroup.type")
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, fmt.Errorf("%w: read cgroup.type: %v", ErrUnsupported, err)
	}
	cgroupType := strings.TrimSpace(string(cgroupTypeData))
	if cgroupType != "domain" {
		_ = unix.Close(rootfd)
		return nil, fmt.Errorf("%w: unsupported cgroup.type %q", ErrUnsupported, cgroupType)
	}
	readOnly := unix.Faccessat(rootfd, ".", unix.W_OK, 0) != nil
	root := RootIdentity{
		HostBootID:        readFirstToken("/proc/sys/kernel/random/boot_id"),
		PIDNamespaceID:    readNamespaceToken("/proc/self/ns/pid"),
		CgroupNamespaceID: readNamespaceToken("/proc/self/ns/cgroup"),
		MountID:           mount.ID,
		HierarchyID:       hierarchyIdentity(statfs),
		Unified:           true,
		ReadOnly:          readOnly,
		Delegated:         !readOnly,
		Exclusive:         exclusive,
		RootObject:        rootObject,
	}
	if root.HostBootID == "" || root.PIDNamespaceID == "" || root.CgroupNamespaceID == "" || root.MountID == "" || root.HierarchyID == "" || !root.RootObject.durable() {
		_ = unix.Close(rootfd)
		return nil, fmt.Errorf("%w: incomplete cgroup domain identity", ErrUnsupported)
	}
	return &realRoot{fd: rootfd, identity: root}, nil
}

func (fs *realFS) CreateChild(ctx context.Context, name string) (cgroupObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRetainedID(name); err != nil {
		return nil, err
	}
	held, err := fs.heldRoot(ctx)
	if err != nil {
		return nil, err
	}
	rootfd := held.fd
	rootObject := held.identity.RootObject
	if err := verifyRootHandle(rootfd, rootObject); err != nil {
		return nil, err
	}
	if err := unix.Mkdirat(rootfd, name, 0o755); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return nil, fmt.Errorf("%w: %w", ErrUnsupported, errLeafCollision)
		}
		return nil, err
	}
	createdLeaf, err := identityAt(rootfd, name)
	if err != nil {
		return nil, err
	}
	leaffd, err := fs.openLeaf(rootfd, name)
	if err != nil {
		fs.removeNameIfMatches(rootfd, name, createdLeaf)
		return nil, err
	}
	leafObject, err := identityFromFD(leaffd)
	if err != nil {
		_ = unix.Close(leaffd)
		fs.removeNameIfMatches(rootfd, name, createdLeaf)
		return nil, err
	}
	if !createdLeaf.durableEqual(leafObject) {
		_ = unix.Close(leaffd)
		fs.removeNameIfMatches(rootfd, name, createdLeaf)
		return nil, fmt.Errorf("%w: created cgroup leaf was replaced before open", ErrUnsupported)
	}
	if err := verifyRootHandle(rootfd, rootObject); err != nil {
		_ = unix.Close(leaffd)
		fs.removeNameIfMatches(rootfd, name, createdLeaf)
		return nil, err
	}
	return &realObject{rootPath: fs.root, leafName: name, rootfd: rootfd, leaffd: leaffd, root: rootObject, leaf: leafObject}, nil
}

func (fs *realFS) Open(ctx context.Context, name string) (cgroupObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRetainedID(name); err != nil {
		return nil, err
	}
	held, err := fs.heldRoot(ctx)
	if err != nil {
		return nil, err
	}
	rootfd := held.fd
	rootObject := held.identity.RootObject
	if err := verifyRootHandle(rootfd, rootObject); err != nil {
		return nil, err
	}
	leaffd, err := fs.openLeaf(rootfd, name)
	if err != nil {
		return nil, err
	}
	leafObject, err := identityFromFD(leaffd)
	if err != nil {
		_ = unix.Close(leaffd)
		return nil, err
	}
	if err := verifyRootHandle(rootfd, rootObject); err != nil {
		_ = unix.Close(leaffd)
		return nil, err
	}
	return &realObject{rootPath: fs.root, leafName: name, rootfd: rootfd, leaffd: leaffd, root: rootObject, leaf: leafObject}, nil
}

func (fs *realFS) Verify(ctx context.Context, object cgroupObject) (bool, error) {
	realObject, err := fs.realObject(ctx, object)
	if err != nil {
		return false, err
	}
	root, err := identityFromFD(realObject.rootfd)
	if err != nil {
		if errors.Is(err, unix.EBADF) || errors.Is(err, unix.ESTALE) {
			return false, nil
		}
		return false, err
	}
	leaf, err := identityFromFD(realObject.leaffd)
	if err != nil {
		if errors.Is(err, unix.EBADF) || errors.Is(err, unix.ESTALE) {
			return false, nil
		}
		return false, err
	}
	if !realObject.root.sameLiveObject(root) || !realObject.leaf.sameLiveObject(leaf) || !realObject.root.durableEqual(root) || !realObject.leaf.durableEqual(leaf) {
		return false, nil
	}
	current, err := identityAt(realObject.rootfd, realObject.leafName)
	if err != nil {
		if isMissingObjectErr(err) {
			return false, nil
		}
		return false, err
	}
	if !realObject.leaf.durableEqual(current) {
		return false, nil
	}
	fd, err := unix.Openat(realObject.leaffd, "cgroup.events", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESTALE) || errors.Is(err, unix.ENOTDIR) {
			return false, nil
		}
		return false, err
	}
	_ = unix.Close(fd)
	return true, nil
}

func (fs *realFS) ProbeFeatures(ctx context.Context, object cgroupObject) (CgroupFeatures, error) {
	features := CgroupFeatures{}
	if err := fs.writeFile(ctx, object, "cgroup.kill", []byte("1\n")); err == nil {
		features.KillAvailable = true
	}
	if err := fs.WriteFreeze(ctx, object, FreezeFrozen); err == nil {
		state, readErr := fs.ReadFreeze(ctx, object)
		if err := fs.WriteFreeze(ctx, object, FreezeThawed); err != nil {
			return features, err
		}
		if readErr == nil && state == FreezeFrozen {
			state, readErr = fs.ReadFreeze(ctx, object)
			if readErr == nil && state == FreezeThawed {
				features.FreezeAvailable = true
			}
		}
	}
	return features, nil
}

func (fs *realFS) WriteProcs(ctx context.Context, object cgroupObject, pid int) error {
	return fs.writeFile(ctx, object, "cgroup.procs", []byte(strconv.Itoa(pid)+"\n"))
}

func (fs *realFS) ReadProcs(ctx context.Context, object cgroupObject) ([]int, error) {
	data, err := fs.readFile(ctx, object, "cgroup.procs")
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(data))
	pids := make([]int, 0, len(fields))
	for _, field := range fields {
		pid, err := strconv.Atoi(field)
		if err != nil {
			return nil, err
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func (fs *realFS) ReadEvents(ctx context.Context, object cgroupObject) (Events, error) {
	data, err := fs.readFile(ctx, object, "cgroup.events")
	if err != nil {
		return Events{}, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "populated" {
			continue
		}
		switch fields[1] {
		case "0":
			return Events{Populated: false}, nil
		case "1":
			return Events{Populated: true}, nil
		default:
			return Events{}, fmt.Errorf("%w: ambiguous cgroup.events populated value", ErrInvalid)
		}
	}
	return Events{}, fmt.Errorf("%w: cgroup.events missing populated", ErrInvalid)
}

func (fs *realFS) WriteKill(ctx context.Context, object cgroupObject) error {
	return fs.writeFile(ctx, object, "cgroup.kill", []byte("1\n"))
}

func (fs *realFS) WriteFreeze(ctx context.Context, object cgroupObject, state FreezeState) error {
	switch state {
	case FreezeFrozen:
		return fs.writeFile(ctx, object, "cgroup.freeze", []byte("1\n"))
	case FreezeThawed:
		return fs.writeFile(ctx, object, "cgroup.freeze", []byte("0\n"))
	default:
		return fmt.Errorf("%w: freeze state is unknown", ErrInvalid)
	}
}

func (fs *realFS) ReadFreeze(ctx context.Context, object cgroupObject) (FreezeState, error) {
	data, err := fs.readFile(ctx, object, "cgroup.freeze")
	if err != nil {
		return FreezeUnknown, err
	}
	switch strings.TrimSpace(string(data)) {
	case "0":
		return FreezeThawed, nil
	case "1":
		return FreezeFrozen, nil
	default:
		return FreezeUnknown, fmt.Errorf("%w: ambiguous cgroup.freeze value", ErrInvalid)
	}
}

func (fs *realFS) Remove(ctx context.Context, object cgroupObject) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	realObject, err := fs.realObject(ctx, object)
	if err != nil {
		return err
	}
	events, err := fs.ReadEvents(ctx, object)
	if err != nil {
		return err
	}
	if events.Populated {
		return ErrPopulated
	}
	held, err := fs.Verify(ctx, object)
	if err != nil {
		return err
	}
	if !held {
		return fmt.Errorf("%w: cgroup object is no longer held", ErrUnsupported)
	}
	return unix.Unlinkat(realObject.rootfd, realObject.leafName, unix.AT_REMOVEDIR)
}

func (fs *realFS) openRoot() (int, error) {
	return unix.Open(fs.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func (fs *realFS) openLeaf(rootfd int, name string) (int, error) {
	if err := validateRetainedID(name); err != nil {
		return -1, err
	}
	return unix.Openat(rootfd, name, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func (fs *realFS) realObject(ctx context.Context, object cgroupObject) (*realObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	realObject, ok := object.(*realObject)
	if !ok || realObject == nil || realObject.closed || realObject.rootPath != fs.root || realObject.rootfd < 0 || realObject.leaffd < 0 {
		return nil, fmt.Errorf("%w: invalid cgroup object", ErrInvalid)
	}
	return realObject, nil
}

func (fs *realFS) readFile(ctx context.Context, object cgroupObject, file string) ([]byte, error) {
	realObject, err := fs.realObject(ctx, object)
	if err != nil {
		return nil, err
	}
	held, err := fs.Verify(ctx, object)
	if err != nil {
		return nil, err
	}
	if !held {
		return nil, fmt.Errorf("%w: cgroup object is no longer held", ErrUnsupported)
	}
	fd, err := unix.Openat(realObject.leaffd, file, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	return readAll(fd)
}

func (fs *realFS) writeFile(ctx context.Context, object cgroupObject, file string, data []byte) error {
	realObject, err := fs.realObject(ctx, object)
	if err != nil {
		return err
	}
	held, err := fs.Verify(ctx, object)
	if err != nil {
		return err
	}
	if !held {
		return fmt.Errorf("%w: cgroup object is no longer held", ErrUnsupported)
	}
	fd, err := unix.Openat(realObject.leaffd, file, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return writeAll(fd, data)
}

func statFD(fd int) (ObjectIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return ObjectIdentity{}, err
	}
	return objectIdentityFromStat(stat), nil
}

func objectIdentityFromStat(stat unix.Stat_t) ObjectIdentity {
	return ObjectIdentity{
		Device:     uint64(stat.Dev),
		Inode:      stat.Ino,
		ChangeSec:  stat.Ctim.Sec,
		ChangeNsec: stat.Ctim.Nsec,
	}
}

func identityFromFD(fd int) (ObjectIdentity, error) {
	identity, err := statFD(fd)
	if err != nil {
		return ObjectIdentity{}, err
	}
	handle, mountID, err := unix.NameToHandleAt(fd, "", unix.AT_EMPTY_PATH)
	if err != nil {
		return ObjectIdentity{}, durableHandleError(err)
	}
	token, err := fileHandleToken(handle, mountID)
	if err != nil {
		return ObjectIdentity{}, err
	}
	identity.Generation = token
	return identity, nil
}

func identityAt(dirfd int, name string) (ObjectIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(dirfd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ObjectIdentity{}, err
	}
	handle, mountID, err := unix.NameToHandleAt(dirfd, name, 0)
	if err != nil {
		return ObjectIdentity{}, durableHandleError(err)
	}
	token, err := fileHandleToken(handle, mountID)
	if err != nil {
		return ObjectIdentity{}, err
	}
	identity := objectIdentityFromStat(stat)
	identity.Generation = token
	return identity, nil
}

func fileHandleToken(handle unix.FileHandle, mountID int) (string, error) {
	handleBytes := handle.Bytes()
	if len(handleBytes) == 0 {
		return "", fmt.Errorf("%w: empty cgroup file handle", ErrUnsupported)
	}
	return fmt.Sprintf("mount=%d,type=%d,bytes=%s", mountID, handle.Type(), hex.EncodeToString(handleBytes)), nil
}

func durableHandleError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: lifetime-unique cgroup handle unavailable: %v", ErrUnsupported, err)
}

func isMissingObjectErr(err error) bool {
	return errors.Is(err, unix.ENOENT) ||
		errors.Is(err, unix.ESTALE) ||
		errors.Is(err, unix.ENOTDIR)
}

func (fs *realFS) removeNameIfMatches(rootfd int, name string, expected ObjectIdentity) {
	current, err := identityAt(rootfd, name)
	if err != nil || !expected.durableEqual(current) {
		return
	}
	_ = unix.Unlinkat(rootfd, name, unix.AT_REMOVEDIR)
}

func (fs *realFS) establishExclusiveDelegation(rootfd int) (bool, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(rootfd, &stat); err != nil {
		return false, err
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Mode&0o022 != 0 {
		return false, nil
	}
	if err := unix.Flock(rootfd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return false, fmt.Errorf("%w: acquire delegated cgroup root lease: %v", ErrUnsupported, err)
	}
	entries, err := readDirAt(rootfd)
	if err != nil {
		return false, fmt.Errorf("%w: inspect delegated cgroup root: %v", ErrUnsupported, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !isManagedLeafName(entry.Name()) {
			_ = unix.Flock(rootfd, unix.LOCK_UN)
			return false, nil
		}
	}
	return true, nil
}

func verifyRootHandle(rootfd int, expected ObjectIdentity) error {
	current, err := identityFromFD(rootfd)
	if err != nil {
		return err
	}
	if !expected.durableEqual(current) {
		return fmt.Errorf("%w: leased cgroup root changed", ErrUnsupported)
	}
	return nil
}

func readRootFile(rootfd int, name string) ([]byte, error) {
	fd, err := unix.Openat(rootfd, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	return readAll(fd)
}

func readDirAt(rootfd int) ([]os.FileInfo, error) {
	fd, err := unix.Openat(rootfd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "cgroup-root")
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: open delegated cgroup root directory", ErrUnsupported)
	}
	defer file.Close()
	return file.Readdir(-1)
}

func readAll(fd int) ([]byte, error) {
	var out []byte
	buffer := make([]byte, 4096)
	for {
		n, err := unix.Read(fd, buffer)
		if n > 0 {
			out = append(out, buffer[:n]...)
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
	}
}

func writeAll(fd int, data []byte) error {
	for len(data) > 0 {
		n, err := unix.Write(fd, data)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		data = data[n:]
	}
	return nil
}

func readFirstToken(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func readNamespaceToken(path string) string {
	target, err := os.Readlink(path)
	if err != nil {
		return ""
	}
	return target
}

func hierarchyIdentity(statfs unix.Statfs_t) string {
	return fmt.Sprintf("type-%x-fsid-%x-%x", uint64(statfs.Type), uint32(statfs.Fsid.Val[0]), uint32(statfs.Fsid.Val[1]))
}

type pidfdTerminator struct{}

type pidfdHandle int

func (pidfdTerminator) Open(pid int) (processHandle, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		if errors.Is(err, unix.ESRCH) {
			return nil, fmt.Errorf("%w: %v", errProcessGone, err)
		}
		return nil, err
	}
	return pidfdHandle(fd), nil
}

func (pidfdTerminator) SendTerm(handle processHandle) error {
	fd, ok := handle.(pidfdHandle)
	if !ok {
		return fmt.Errorf("%w: invalid pidfd handle", ErrInvalid)
	}
	return unix.PidfdSendSignal(int(fd), unix.SIGTERM, nil, 0)
}

func (pidfdTerminator) Close(handle processHandle) error {
	fd, ok := handle.(pidfdHandle)
	if !ok {
		return nil
	}
	return unix.Close(int(fd))
}

//go:build linux

package cgroup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	return newManagerWithFS(realFS{root: filepath.Clean(abs)}, managerOptions{
		terminator: pidfdTerminator{},
	}), nil
}

type realFS struct {
	root string
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
		if closeErr := unix.Close(object.rootfd); err == nil && closeErr != nil {
			err = closeErr
		}
		object.rootfd = -1
	}
	return err
}

func (fs realFS) RootIdentity(ctx context.Context) (RootIdentity, error) {
	if err := ctx.Err(); err != nil {
		return RootIdentity{}, err
	}
	var statfs unix.Statfs_t
	if err := unix.Statfs(fs.root, &statfs); err != nil {
		return RootIdentity{}, fmt.Errorf("%w: cgroupfs absent: %v", ErrUnsupported, err)
	}
	if statfs.Type != unix.CGROUP2_SUPER_MAGIC {
		return RootIdentity{}, fmt.Errorf("%w: cgroup root is not cgroup2", ErrUnsupported)
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return RootIdentity{}, err
	}
	defer unix.Close(rootfd)
	rootObject, err := statFD(rootfd)
	if err != nil {
		return RootIdentity{}, err
	}
	selfCgroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return RootIdentity{}, fmt.Errorf("%w: read /proc/self/cgroup: %v", ErrUnsupported, err)
	}
	mountData, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return RootIdentity{}, fmt.Errorf("%w: read /proc/self/mountinfo: %v", ErrUnsupported, err)
	}
	mounts, err := parseMountInfo(string(mountData))
	if err != nil {
		return RootIdentity{}, err
	}
	mount, err := classifyUnifiedCgroupRoot(string(selfCgroup), mounts, fs.root)
	if err != nil {
		return RootIdentity{}, err
	}
	cgroupTypeData, err := os.ReadFile(filepath.Join(fs.root, "cgroup.type"))
	if err != nil {
		return RootIdentity{}, fmt.Errorf("%w: read cgroup.type: %v", ErrUnsupported, err)
	}
	cgroupType := strings.TrimSpace(string(cgroupTypeData))
	if cgroupType != "domain" {
		return RootIdentity{}, fmt.Errorf("%w: unsupported cgroup.type %q", ErrUnsupported, cgroupType)
	}
	readOnly := unix.Access(fs.root, unix.W_OK) != nil
	root := RootIdentity{
		HostBootID:        readFirstToken("/proc/sys/kernel/random/boot_id"),
		PIDNamespaceID:    readNamespaceToken("/proc/self/ns/pid"),
		CgroupNamespaceID: readNamespaceToken("/proc/self/ns/cgroup"),
		MountID:           mount.ID,
		HierarchyID:       hierarchyIdentity(statfs),
		Unified:           true,
		ReadOnly:          readOnly,
		Delegated:         !readOnly,
		RootObject:        rootObject,
	}
	if root.HostBootID == "" || root.PIDNamespaceID == "" || root.CgroupNamespaceID == "" || root.MountID == "" || root.HierarchyID == "" || !root.RootObject.valid() {
		return RootIdentity{}, fmt.Errorf("%w: incomplete cgroup domain identity", ErrUnsupported)
	}
	return root, nil
}

func (fs realFS) CreateChild(ctx context.Context, name string) (cgroupObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRetainedID(name); err != nil {
		return nil, err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return nil, err
	}
	rootObject, err := statFD(rootfd)
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	if err := unix.Mkdirat(rootfd, name, 0o755); err != nil {
		_ = unix.Close(rootfd)
		if errors.Is(err, unix.EEXIST) {
			return nil, fmt.Errorf("%w: leaf already exists", ErrUnsupported)
		}
		return nil, err
	}
	leaffd, err := fs.openLeaf(rootfd, name)
	if err != nil {
		_ = unix.Unlinkat(rootfd, name, unix.AT_REMOVEDIR)
		_ = unix.Close(rootfd)
		return nil, err
	}
	leafObject, err := statFD(leaffd)
	if err != nil {
		_ = unix.Close(leaffd)
		_ = unix.Unlinkat(rootfd, name, unix.AT_REMOVEDIR)
		_ = unix.Close(rootfd)
		return nil, err
	}
	return &realObject{rootPath: fs.root, leafName: name, rootfd: rootfd, leaffd: leaffd, root: rootObject, leaf: leafObject}, nil
}

func (fs realFS) Open(ctx context.Context, name string) (cgroupObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRetainedID(name); err != nil {
		return nil, err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return nil, err
	}
	rootObject, err := statFD(rootfd)
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	leaffd, err := fs.openLeaf(rootfd, name)
	if err != nil {
		_ = unix.Close(rootfd)
		return nil, err
	}
	leafObject, err := statFD(leaffd)
	if err != nil {
		_ = unix.Close(leaffd)
		_ = unix.Close(rootfd)
		return nil, err
	}
	return &realObject{rootPath: fs.root, leafName: name, rootfd: rootfd, leaffd: leaffd, root: rootObject, leaf: leafObject}, nil
}

func (fs realFS) Verify(ctx context.Context, object cgroupObject) (bool, error) {
	realObject, err := fs.realObject(ctx, object)
	if err != nil {
		return false, err
	}
	root, err := statFD(realObject.rootfd)
	if err != nil {
		if errors.Is(err, unix.EBADF) || errors.Is(err, unix.ESTALE) {
			return false, nil
		}
		return false, err
	}
	leaf, err := statFD(realObject.leaffd)
	if err != nil {
		if errors.Is(err, unix.EBADF) || errors.Is(err, unix.ESTALE) {
			return false, nil
		}
		return false, err
	}
	if !realObject.root.Equal(root) || !realObject.leaf.Equal(leaf) {
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

func (fs realFS) ProbeFeatures(ctx context.Context, object cgroupObject) (CgroupFeatures, error) {
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

func (fs realFS) WriteProcs(ctx context.Context, object cgroupObject, pid int) error {
	return fs.writeFile(ctx, object, "cgroup.procs", []byte(strconv.Itoa(pid)+"\n"))
}

func (fs realFS) ReadProcs(ctx context.Context, object cgroupObject) ([]int, error) {
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

func (fs realFS) ReadEvents(ctx context.Context, object cgroupObject) (Events, error) {
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

func (fs realFS) WriteKill(ctx context.Context, object cgroupObject) error {
	return fs.writeFile(ctx, object, "cgroup.kill", []byte("1\n"))
}

func (fs realFS) WriteFreeze(ctx context.Context, object cgroupObject, state FreezeState) error {
	switch state {
	case FreezeFrozen:
		return fs.writeFile(ctx, object, "cgroup.freeze", []byte("1\n"))
	case FreezeThawed:
		return fs.writeFile(ctx, object, "cgroup.freeze", []byte("0\n"))
	default:
		return fmt.Errorf("%w: freeze state is unknown", ErrInvalid)
	}
}

func (fs realFS) ReadFreeze(ctx context.Context, object cgroupObject) (FreezeState, error) {
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

func (fs realFS) Remove(ctx context.Context, object cgroupObject) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	return unix.Unlinkat(realObject.rootfd, realObject.leafName, unix.AT_REMOVEDIR)
}

func (fs realFS) openRoot() (int, error) {
	return unix.Open(fs.root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
}

func (fs realFS) openLeaf(rootfd int, name string) (int, error) {
	if err := validateRetainedID(name); err != nil {
		return -1, err
	}
	return unix.Openat(rootfd, name, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func (fs realFS) realObject(ctx context.Context, object cgroupObject) (*realObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	realObject, ok := object.(*realObject)
	if !ok || realObject == nil || realObject.closed || realObject.rootPath != fs.root || realObject.rootfd < 0 || realObject.leaffd < 0 {
		return nil, fmt.Errorf("%w: invalid cgroup object", ErrInvalid)
	}
	return realObject, nil
}

func (fs realFS) readFile(ctx context.Context, object cgroupObject, file string) ([]byte, error) {
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

func (fs realFS) writeFile(ctx context.Context, object cgroupObject, file string, data []byte) error {
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
	return ObjectIdentity{
		Device:     uint64(stat.Dev),
		Inode:      stat.Ino,
		ChangeSec:  stat.Ctim.Sec,
		ChangeNsec: stat.Ctim.Nsec,
	}, nil
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

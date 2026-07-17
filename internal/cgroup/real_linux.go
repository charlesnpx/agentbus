//go:build linux

package cgroup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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

func (fs realFS) RootIdentity(ctx context.Context) (RootIdentity, error) {
	if err := ctx.Err(); err != nil {
		return RootIdentity{}, err
	}
	var statfs unix.Statfs_t
	if err := unix.Statfs(fs.root, &statfs); err != nil {
		return RootIdentity{}, fmt.Errorf("%w: cgroupfs absent: %v", ErrUnsupported, err)
	}
	unified := statfs.Type == unix.CGROUP2_SUPER_MAGIC
	if statfs.Type == unix.CGROUP_SUPER_MAGIC {
		unified = false
	}
	readOnly := unix.Access(fs.root, unix.W_OK) != nil
	threaded := false
	if data, err := os.ReadFile(filepath.Join(fs.root, "cgroup.type")); err == nil {
		threaded = strings.TrimSpace(string(data)) == "threaded"
	}
	root := RootIdentity{
		HostBootID:        readFirstToken("/proc/sys/kernel/random/boot_id"),
		PIDNamespaceID:    readNamespaceToken("/proc/self/ns/pid"),
		CgroupNamespaceID: readNamespaceToken("/proc/self/ns/cgroup"),
		MountID:           mountIdentity(fs.root),
		HierarchyID:       hierarchyIdentity(statfs),
		Unified:           unified,
		ReadOnly:          readOnly,
		Delegated:         !readOnly,
		Threaded:          threaded,
		KillAvailable:     unified,
		FreezeAvailable:   fileExists(filepath.Join(fs.root, "cgroup.freeze")),
	}
	if root.HostBootID == "" || root.PIDNamespaceID == "" || root.CgroupNamespaceID == "" || root.MountID == "" || root.HierarchyID == "" {
		return RootIdentity{}, fmt.Errorf("%w: incomplete cgroup domain identity", ErrUnsupported)
	}
	return root, nil
}

func (fs realFS) CreateChild(ctx context.Context, name string) (ObjectIdentity, error) {
	if err := ctx.Err(); err != nil {
		return ObjectIdentity{}, err
	}
	if err := validateRetainedID(name); err != nil {
		return ObjectIdentity{}, err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return ObjectIdentity{}, err
	}
	defer unix.Close(rootfd)
	if err := unix.Mkdirat(rootfd, name, 0o755); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return ObjectIdentity{}, fmt.Errorf("%w: leaf already exists", ErrUnsupported)
		}
		return ObjectIdentity{}, err
	}
	return statLeaf(rootfd, name)
}

func (fs realFS) Open(ctx context.Context, name string) (ObjectIdentity, error) {
	if err := ctx.Err(); err != nil {
		return ObjectIdentity{}, err
	}
	if err := validateRetainedID(name); err != nil {
		return ObjectIdentity{}, err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return ObjectIdentity{}, err
	}
	defer unix.Close(rootfd)
	return statLeaf(rootfd, name)
}

func (fs realFS) WriteProcs(ctx context.Context, name string, pid int) error {
	return fs.writeFile(ctx, name, "cgroup.procs", []byte(strconv.Itoa(pid)+"\n"))
}

func (fs realFS) ReadProcs(ctx context.Context, name string) ([]int, error) {
	data, err := fs.readFile(ctx, name, "cgroup.procs")
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

func (fs realFS) ReadEvents(ctx context.Context, name string) (Events, error) {
	data, err := fs.readFile(ctx, name, "cgroup.events")
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

func (fs realFS) WriteKill(ctx context.Context, name string) error {
	return fs.writeFile(ctx, name, "cgroup.kill", []byte("1\n"))
}

func (fs realFS) WriteFreeze(ctx context.Context, name string, state FreezeState) error {
	switch state {
	case FreezeFrozen:
		return fs.writeFile(ctx, name, "cgroup.freeze", []byte("1\n"))
	case FreezeThawed:
		return fs.writeFile(ctx, name, "cgroup.freeze", []byte("0\n"))
	default:
		return fmt.Errorf("%w: freeze state is unknown", ErrInvalid)
	}
}

func (fs realFS) ReadFreeze(ctx context.Context, name string) (FreezeState, error) {
	data, err := fs.readFile(ctx, name, "cgroup.freeze")
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

func (fs realFS) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRetainedID(name); err != nil {
		return err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return err
	}
	defer unix.Close(rootfd)
	return unix.Unlinkat(rootfd, name, unix.AT_REMOVEDIR)
}

func (fs realFS) openRoot() (int, error) {
	return unix.Open(fs.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
}

func (fs realFS) openLeaf(rootfd int, name string) (int, error) {
	if err := validateRetainedID(name); err != nil {
		return -1, err
	}
	return unix.Openat(rootfd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func (fs realFS) readFile(ctx context.Context, name, file string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootfd)
	leaffd, err := fs.openLeaf(rootfd, name)
	if err != nil {
		return nil, err
	}
	defer unix.Close(leaffd)
	fd, err := unix.Openat(leaffd, file, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	return readAll(fd)
}

func (fs realFS) writeFile(ctx context.Context, name, file string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rootfd, err := fs.openRoot()
	if err != nil {
		return err
	}
	defer unix.Close(rootfd)
	leaffd, err := fs.openLeaf(rootfd, name)
	if err != nil {
		return err
	}
	defer unix.Close(leaffd)
	fd, err := unix.Openat(leaffd, file, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return writeAll(fd, data)
}

func statLeaf(rootfd int, name string) (ObjectIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(rootfd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func hierarchyIdentity(statfs unix.Statfs_t) string {
	return fmt.Sprintf("type-%x-fsid-%x-%x", uint64(statfs.Type), uint32(statfs.Fsid.Val[0]), uint32(statfs.Fsid.Val[1]))
}

func mountIdentity(root string) string {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return ""
	}
	defer file.Close()
	root = filepath.Clean(root)
	var bestID string
	bestLen := -1
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			fields := strings.Fields(line)
			if len(fields) >= 5 {
				mountPoint := unescapeMountInfo(fields[4])
				if root == mountPoint || strings.HasPrefix(root, mountPoint+"/") {
					if len(mountPoint) > bestLen {
						bestLen = len(mountPoint)
						bestID = fields[0]
					}
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return ""
		}
	}
	return bestID
}

func unescapeMountInfo(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
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

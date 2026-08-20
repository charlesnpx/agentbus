package jobstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"runtime/debug"

	bolt "go.etcd.io/bbolt"
)

const (
	boltPageHeaderSize   = 16
	boltMetaPayloadSize  = 64
	boltMagic            = 0xED0CDAED
	boltVersion          = 2
	boltMinPageSize      = 512
	boltMaxPageSize      = 128 * 1024
	boltBranchPageFlag   = 0x01
	boltLeafPageFlag     = 0x02
	boltMetaPageFlag     = 0x04
	boltFreelistPageFlag = 0x10

	boltStructuralGraphPageFloor = 2
	boltNoFreelistID             = ^uint64(0)
)

type boltPreflightMeta struct {
	pageSize uint64
	root     uint64
	freelist uint64
	pgid     uint64
	txid     uint64
}

type boltPreflightPage struct {
	id       uint64
	flags    uint16
	count    uint16
	overflow uint32
}

type boltFreelistPageInfo struct {
	span      uint64
	count     uint64
	idsOffset uint64
}

func preflightBoltPageHeaders(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	meta, ok, err := readBoltPreflightMeta(file, size)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: bbolt meta pages are invalid: %s", ErrCorrupt, path)
	}
	if meta.pgid < 2 {
		return fmt.Errorf("%w: bbolt high water mark %d is invalid: %s", ErrCorrupt, meta.pgid, path)
	}
	if meta.pageSize == 0 || uint64(size)/meta.pageSize < meta.pgid {
		return fmt.Errorf("%w: bbolt file is truncated: pages=%d page_size=%d size=%d: %s", ErrCorrupt, meta.pgid, meta.pageSize, size, path)
	}
	// Deliberately NOT a linear physical scan asserting page.id == pageID for every
	// page. bbolt does not guarantee that: overflow-continuation pages carry raw
	// payload with no self-identifying header, and freed pages retain stale
	// overflow/flags/id from a prior allocation. A physical linear scan therefore
	// spuriously rejects VALID databases — especially with small OS page sizes (e.g.
	// 4KiB on Linux), where the same data needs overflow spans that are absent on a
	// 16KiB darwin page. Sound structural validation is reachability-based and lives
	// elsewhere: meta pages are validated above and the freelist in
	// preflightBoltFreelist (both before open); openBoltSafely wraps bolt.Open in
	// fault recovery; and the store runs bbolt's integrity check after opening to
	// walk its live b-tree and cross-check the freelist against reachable pages.
	return nil
}

func preflightBoltFreelist(path string) (err error) {
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if boltPanicIsCorruption(recovered) {
				err = fmt.Errorf("%w: bbolt freelist preflight fault for %s: %v", ErrCorrupt, path, recovered)
				return
			}
			panic(recovered)
		}
	}()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size < 0 {
		return fmt.Errorf("%w: bbolt file has invalid size %d: %s", ErrCorrupt, size, path)
	}
	meta, ok, err := readBoltPreflightMeta(file, size)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: bbolt meta pages are invalid: %s", ErrCorrupt, path)
	}
	return validateBoltFreelistPage(file, meta, uint64(size), path)
}

func validateBoltFreelistPage(file *os.File, meta boltPreflightMeta, fileSize uint64, path string) error {
	if meta.freelist == boltNoFreelistID {
		return nil
	}
	if err := validateBoltFreelistPageID(meta, path); err != nil {
		return err
	}
	if meta.pageSize == 0 || fileSize/meta.pageSize < meta.pgid {
		return fmt.Errorf("%w: bbolt freelist preflight file is truncated: pages=%d page_size=%d size=%d: %s", ErrCorrupt, meta.pgid, meta.pageSize, fileSize, path)
	}
	page, err := readBoltPreflightPage(file, meta.freelist, meta.pageSize)
	if err != nil {
		return fmt.Errorf("%w: bbolt freelist page %d header read failed: %v", ErrCorrupt, meta.freelist, err)
	}
	if page.id != meta.freelist {
		return fmt.Errorf("%w: bbolt freelist page %d self-identifies as %d: %s", ErrCorrupt, meta.freelist, page.id, path)
	}
	if page.flags != boltFreelistPageFlag {
		return fmt.Errorf("%w: bbolt freelist page %d has flags 0x%x, want freelist: %s", ErrCorrupt, meta.freelist, page.flags, path)
	}
	span, err := validateBoltFreelistSpan(meta, page, path)
	if err != nil {
		return err
	}
	start, end, ok := checkedBoltDataRange(meta.freelist, meta.pageSize, span, fileSize)
	if !ok {
		return fmt.Errorf("%w: bbolt freelist page %d overflow %d is outside file data: %s", ErrCorrupt, meta.freelist, page.overflow, path)
	}
	spanBytes := uint64(end - start)
	startOffset := uint64(start)
	info, err := validateBoltFreelistCount(meta, page, span, spanBytes, path, func() (uint64, error) {
		var buf [8]byte
		if _, err := file.ReadAt(buf[:], int64(startOffset+boltPageHeaderSize)); err != nil {
			return 0, fmt.Errorf("%w: bbolt freelist page %d extended count read failed: %v", ErrCorrupt, meta.freelist, err)
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	})
	if err != nil {
		return err
	}
	return validateBoltFreelistIDs(meta, info, path, func(index uint64) (uint64, error) {
		var buf [8]byte
		offset := startOffset + info.idsOffset + index*8
		if _, err := file.ReadAt(buf[:], int64(offset)); err != nil {
			return 0, fmt.Errorf("%w: bbolt freelist page %d entry %d read failed: %v", ErrCorrupt, meta.freelist, index, err)
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	})
}

func validateBoltFreelistPageID(meta boltPreflightMeta, path string) error {
	if meta.freelist < boltStructuralGraphPageFloor || meta.freelist >= meta.pgid {
		return fmt.Errorf("%w: bbolt freelist page %d is out of range [2,%d): %s", ErrCorrupt, meta.freelist, meta.pgid, path)
	}
	return nil
}

func validateBoltFreelistSpan(meta boltPreflightMeta, page boltPreflightPage, path string) (uint64, error) {
	span := uint64(page.overflow) + 1
	if span == 0 || span > meta.pgid || meta.freelist > meta.pgid-span {
		return 0, fmt.Errorf("%w: bbolt freelist page %d overflow %d exceeds high water mark %d: %s", ErrCorrupt, meta.freelist, page.overflow, meta.pgid, path)
	}
	return span, nil
}

func validateBoltFreelistCount(meta boltPreflightMeta, page boltPreflightPage, span, spanBytes uint64, path string, readExtendedCount func() (uint64, error)) (boltFreelistPageInfo, error) {
	if spanBytes < boltPageHeaderSize {
		return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d span is shorter than a page header: %s", ErrCorrupt, meta.freelist, path)
	}
	capacity := (spanBytes - boltPageHeaderSize) / 8
	count := uint64(page.count)
	requiredSlots := count
	idsOffset := uint64(boltPageHeaderSize)
	if page.count == 0xffff {
		if capacity == 0 {
			return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d extended count has no storage: %s", ErrCorrupt, meta.freelist, path)
		}
		extendedCount, err := readExtendedCount()
		if err != nil {
			return boltFreelistPageInfo{}, err
		}
		count = extendedCount
		if count > uint64(^uint(0)>>1) {
			return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d extended count %d exceeds addressable memory: %s", ErrCorrupt, meta.freelist, count, path)
		}
		if count == ^uint64(0) {
			return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d extended count overflows storage slots: %s", ErrCorrupt, meta.freelist, path)
		}
		requiredSlots = count + 1
		idsOffset += 8
	}
	if requiredSlots > capacity {
		return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d count %d requires %d slots, capacity %d: %s", ErrCorrupt, meta.freelist, count, requiredSlots, capacity, path)
	}
	if count > meta.pgid {
		return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d count %d exceeds high water mark %d: %s", ErrCorrupt, meta.freelist, count, meta.pgid, path)
	}
	return boltFreelistPageInfo{
		span:      span,
		count:     count,
		idsOffset: idsOffset,
	}, nil
}

func validateBoltFreelistIDs(meta boltPreflightMeta, info boltFreelistPageInfo, path string, readID func(index uint64) (uint64, error)) error {
	seen := make(map[uint64]struct{}, int(info.count))
	freelistEnd := meta.freelist + info.span
	for i := uint64(0); i < info.count; i++ {
		id, err := readID(i)
		if err != nil {
			return err
		}
		if id < boltStructuralGraphPageFloor || id >= meta.pgid {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d page %d is out of range [2,%d): %s", ErrCorrupt, meta.freelist, i, id, meta.pgid, path)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d page %d is duplicated: %s", ErrCorrupt, meta.freelist, i, id, path)
		}
		seen[id] = struct{}{}
		if id >= meta.freelist && id < freelistEnd {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d lists freelist span page %d in [%d,%d): %s", ErrCorrupt, meta.freelist, i, id, meta.freelist, freelistEnd, path)
		}
	}
	return nil
}

func readBoltPreflightMeta(file *os.File, size int64) (boltPreflightMeta, bool, error) {
	if meta, ok, err := readBoltPreflightMetaAt(file, 0, 0); err != nil {
		return boltPreflightMeta{}, false, err
	} else if ok {
		if second, secondOK, secondErr := readBoltPreflightMetaAt(file, 1, meta.pageSize); secondErr != nil {
			return boltPreflightMeta{}, false, secondErr
		} else if secondOK {
			if second.pageSize != meta.pageSize {
				return boltPreflightMeta{}, false, fmt.Errorf("%w: bbolt meta pages disagree on page size: page0=%d page1=%d", ErrCorrupt, meta.pageSize, second.pageSize)
			}
			if second.txid > meta.txid {
				return second, true, nil
			}
			return meta, true, nil
		}
		if size >= 0 && uint64(size) < meta.pageSize+boltPageHeaderSize+boltMetaPayloadSize {
			return boltPreflightMeta{}, false, fmt.Errorf("%w: bbolt meta page 1 at offset %d exceeds file size %d", ErrCorrupt, meta.pageSize, size)
		}
		return boltPreflightMeta{}, false, nil
	}
	return boltPreflightMeta{}, false, nil
}

func readBoltPreflightMetaAt(file *os.File, pageID, offset uint64) (boltPreflightMeta, bool, error) {
	buf := make([]byte, boltPageHeaderSize+boltMetaPayloadSize)
	n, err := file.ReadAt(buf, int64(offset))
	if err != nil && n != len(buf) {
		return boltPreflightMeta{}, false, nil
	}
	page := decodeBoltPreflightPage(buf)
	if page.id != pageID || page.flags != boltMetaPageFlag {
		return boltPreflightMeta{}, false, nil
	}
	meta := buf[boltPageHeaderSize:]
	if binary.LittleEndian.Uint32(meta[0:4]) != boltMagic || binary.LittleEndian.Uint32(meta[4:8]) != boltVersion {
		return boltPreflightMeta{}, false, nil
	}
	pageSize := uint64(binary.LittleEndian.Uint32(meta[8:12]))
	if !plausibleBoltPageSize(pageSize) {
		return boltPreflightMeta{}, false, nil
	}
	if !boltMetaChecksumMatches(meta) {
		return boltPreflightMeta{}, false, nil
	}
	return boltPreflightMeta{
		pageSize: pageSize,
		root:     binary.LittleEndian.Uint64(meta[16:24]),
		freelist: binary.LittleEndian.Uint64(meta[32:40]),
		pgid:     binary.LittleEndian.Uint64(meta[40:48]),
		txid:     binary.LittleEndian.Uint64(meta[48:56]),
	}, true, nil
}

func readBoltPreflightPage(file *os.File, pageID, pageSize uint64) (boltPreflightPage, error) {
	buf := make([]byte, boltPageHeaderSize)
	n, err := file.ReadAt(buf, int64(pageID*pageSize))
	if err != nil && n != len(buf) {
		return boltPreflightPage{}, err
	}
	if n != len(buf) {
		return boltPreflightPage{}, fmt.Errorf("short page header read: %d/%d", n, len(buf))
	}
	return decodeBoltPreflightPage(buf), nil
}

func decodeBoltPreflightPage(buf []byte) boltPreflightPage {
	return boltPreflightPage{
		id:       binary.LittleEndian.Uint64(buf[0:8]),
		flags:    binary.LittleEndian.Uint16(buf[8:10]),
		count:    binary.LittleEndian.Uint16(buf[10:12]),
		overflow: binary.LittleEndian.Uint32(buf[12:16]),
	}
}

func plausibleBoltPageSize(size uint64) bool {
	return size >= boltMinPageSize && size <= boltMaxPageSize && size&(size-1) == 0
}

func boltMetaChecksumMatches(meta []byte) bool {
	if len(meta) < boltMetaPayloadSize {
		return false
	}
	hash := fnv.New64a()
	_, _ = hash.Write(meta[:56])
	return hash.Sum64() == binary.LittleEndian.Uint64(meta[56:64])
}

func checkedBoltDataRange(pgid, pageSize, span, dataLen uint64) (int, int, bool) {
	if pageSize == 0 || span == 0 {
		return 0, 0, false
	}
	if pgid > ^uint64(0)/pageSize || span > ^uint64(0)/pageSize {
		return 0, 0, false
	}
	start := pgid * pageSize
	size := span * pageSize
	if start > ^uint64(0)-size {
		return 0, 0, false
	}
	end := start + size
	if start > dataLen || end > dataLen {
		return 0, 0, false
	}
	if start > uint64(^uint(0)>>1) || end > uint64(^uint(0)>>1) {
		return 0, 0, false
	}
	return int(start), int(end), true
}

func boltPanicIsCorruption(recovered any) bool {
	if recovered == nil {
		return false
	}
	if _, ok := recovered.(interface {
		runtime.Error
		Addr() uintptr
	}); ok {
		return true
	}
	err, ok := recovered.(error)
	return ok && boltErrorIsCorruption(err)
}

func boltErrorIsCorruption(err error) bool {
	return errors.Is(err, bolt.ErrInvalid) ||
		errors.Is(err, bolt.ErrInvalidMapping) ||
		errors.Is(err, bolt.ErrVersionMismatch) ||
		errors.Is(err, bolt.ErrChecksum)
}

func openBoltSafely(path string, mode os.FileMode, options *bolt.Options) (db *bolt.DB, err error) {
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if db != nil {
				_ = db.Close()
			}
			db = nil
			if boltPanicIsCorruption(recovered) {
				err = fmt.Errorf("%w: bbolt open fault for %s: %v", ErrCorrupt, path, recovered)
				return
			}
			panic(recovered)
		}
	}()
	return bolt.Open(path, mode, options)
}

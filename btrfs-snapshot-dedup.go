package main

/*
#include <linux/btrfs.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <stddef.h>

// Export ioctl numbers as constants — cgo can't evaluate macros directly
const unsigned long IOCTL_GET_SUBVOL_INFO = BTRFS_IOC_GET_SUBVOL_INFO;
const unsigned long IOCTL_TREE_SEARCH_V2 = BTRFS_IOC_TREE_SEARCH_V2;
const unsigned long IOCTL_INO_LOOKUP = BTRFS_IOC_INO_LOOKUP;
const unsigned long IOCTL_FIEMAP = FS_IOC_FIEMAP;
const unsigned long IOCTL_FIDEDUPERANGE = FIDEDUPERANGE;
const unsigned long IOCTL_LOGICAL_INO_V2 = BTRFS_IOC_LOGICAL_INO_V2;

const int DEDUPE_RANGE_SIZE = sizeof(struct file_dedupe_range);
const int DEDUPE_RANGE_INFO_SIZE = sizeof(struct file_dedupe_range_info);
*/
import "C"

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	IOC_GET_SUBVOL_INFO = uintptr(C.IOCTL_GET_SUBVOL_INFO)
	IOC_TREE_SEARCH_V2  = uintptr(C.IOCTL_TREE_SEARCH_V2)
	IOC_INO_LOOKUP      = uintptr(C.IOCTL_INO_LOOKUP)
	IOC_FIEMAP          = uintptr(C.IOCTL_FIEMAP)
	IOC_FIDEDUPERANGE   = uintptr(C.IOCTL_FIDEDUPERANGE)
	IOC_LOGICAL_INO_V2  = uintptr(C.IOCTL_LOGICAL_INO_V2)

	DEDUPE_RANGE_SIZE      = int(C.DEDUPE_RANGE_SIZE)
	DEDUPE_RANGE_INFO_SIZE = int(C.DEDUPE_RANGE_INFO_SIZE)
)

const VERSION = "1.4.0"

const (
	QUEUE_LIMIT    = 10000
	DEDUP_WORKERS  = 4

	SEARCH_KEY_SIZE    = C.sizeof_struct_btrfs_ioctl_search_key
	SEARCH_HEADER_SIZE = 32 // btrfs_ioctl_search_header is not in uapi, always 32

	BTRFS_ROOT_ITEM_KEY = 132

	FIEMAP_HEADER_SIZE = C.sizeof_struct_fiemap
	FIEMAP_EXTENT_SIZE = C.sizeof_struct_fiemap_extent

	FIEMAP_EXTENT_ENCODED = 0x00000008 // compressed/encoded extent
)

// SpillQueue: in-memory queue that spills to a temp file when full.
// Producer pushes fast (counted immediately), consumer pops from memory or refills from disk.
type SpillQueue struct {
	mu        sync.Mutex
	queue     []string // in-memory buffer
	limit     int
	spillFile *os.File
	spillW    *bufio.Writer
	spillPath string
	spillR    *bufio.Scanner
	spilled   int64 // entries written to disk
	refilled  int64 // entries read back from disk
	total     atomic.Int64
	closed    bool
}

func NewSpillQueue(limit int) *SpillQueue {
	f, _ := os.CreateTemp("/tmp", "spillqueue-*.bin")
	os.Chmod(f.Name(), 0644)
	return &SpillQueue{
		limit:     limit,
		spillFile: f,
		spillW:    bufio.NewWriter(f),
		spillPath: f.Name(),
	}
}

func (q *SpillQueue) Push(s string) {
	q.total.Add(1)
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) < q.limit {
		q.queue = append(q.queue, s)
	} else {
		// Spill to disk
		fmt.Fprintln(q.spillW, s)
		q.spilled++
	}
}

func (q *SpillQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.spillW.Flush()
	q.closed = true
}

func (q *SpillQueue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) > 0 {
		s := q.queue[0]
		q.queue = q.queue[1:]
		return s, true
	}

	// Refill from spill file
	if q.spillR == nil && q.spilled > q.refilled {
		// Open spill file for reading from beginning
		q.spillFile.Seek(0, 0)
		q.spillR = bufio.NewScanner(q.spillFile)
		// Skip already-refilled lines
		for i := int64(0); i < q.refilled && q.spillR.Scan(); i++ {
		}
	}

	if q.spillR != nil && q.spillR.Scan() {
		q.refilled++
		return q.spillR.Text(), true
	}

	if q.closed {
		return "", false
	}
	return "", false // empty but not closed — caller should retry
}

func (q *SpillQueue) Total() int64 {
	return q.total.Load()
}

func (q *SpillQueue) Buffered() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue) + int(q.spilled-q.refilled)
}

func (q *SpillQueue) Cleanup() {
	q.spillFile.Close()
	os.Remove(q.spillPath)
}

func getSubvolInfo(fd int) (*C.struct_btrfs_ioctl_get_subvol_info_args, error) {
	var info C.struct_btrfs_ioctl_get_subvol_info_args
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		IOC_GET_SUBVOL_INFO, uintptr(unsafe.Pointer(&info)))
	if errno != 0 {
		return nil, errno
	}
	return &info, nil
}

func fmtTime(seconds int) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%dm%02ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh%02dm", seconds/3600, (seconds%3600)/60)
}

// Get physical offset and length of first extent via FIEMAP ioctl
func getFirstExtentPhys(path string) (uint64, uint64, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return 0, 0, err
	}
	defer syscall.Close(fd)

	buf := make([]byte, FIEMAP_HEADER_SIZE+FIEMAP_EXTENT_SIZE)
	binary.LittleEndian.PutUint64(buf[0:8], 0)           // fm_start
	binary.LittleEndian.PutUint64(buf[8:16], ^uint64(0))  // fm_length
	binary.LittleEndian.PutUint32(buf[24:28], 1)          // fm_extent_count

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		IOC_FIEMAP, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return 0, 0, errno
	}

	mapped := binary.LittleEndian.Uint32(buf[20:24])
	if mapped == 0 {
		return 0, 0, fmt.Errorf("no extents")
	}

	phys := binary.LittleEndian.Uint64(buf[FIEMAP_HEADER_SIZE+8 : FIEMAP_HEADER_SIZE+16])
	length := binary.LittleEndian.Uint64(buf[FIEMAP_HEADER_SIZE+16 : FIEMAP_HEADER_SIZE+24])
	return phys, length, nil
}

// extentInfo holds metadata about a file's extents for source selection
type extentInfo struct {
	phys       uint64
	physLen    uint64
	compressed bool
	numExtents int
}

// getExtentInfo returns physical offset, compressed flag, and extent count.
// Uses a single FIEMAP call with room for up to 256 extents.
func getExtentInfo(path string) (extentInfo, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return extentInfo{}, err
	}
	defer syscall.Close(fd)

	const maxExtents = 256
	buf := make([]byte, FIEMAP_HEADER_SIZE+FIEMAP_EXTENT_SIZE*maxExtents)
	binary.LittleEndian.PutUint64(buf[0:8], 0)              // fm_start
	binary.LittleEndian.PutUint64(buf[8:16], ^uint64(0))     // fm_length
	binary.LittleEndian.PutUint32(buf[24:28], maxExtents)    // fm_extent_count

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		IOC_FIEMAP, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return extentInfo{}, errno
	}

	mapped := binary.LittleEndian.Uint32(buf[20:24])
	if mapped == 0 {
		return extentInfo{}, fmt.Errorf("no extents")
	}

	// First extent: phys offset, length, flags
	phys := binary.LittleEndian.Uint64(buf[FIEMAP_HEADER_SIZE+8 : FIEMAP_HEADER_SIZE+16])
	physLen := binary.LittleEndian.Uint64(buf[FIEMAP_HEADER_SIZE+16 : FIEMAP_HEADER_SIZE+24])
	flags := binary.LittleEndian.Uint32(buf[FIEMAP_HEADER_SIZE+40 : FIEMAP_HEADER_SIZE+44])
	compressed := flags&FIEMAP_EXTENT_ENCODED != 0

	return extentInfo{
		phys:       phys,
		physLen:    physLen,
		compressed: compressed,
		numExtents: int(mapped),
	}, nil
}

func fmtBytesStatic(b int64) string {
	if b >= 1024*1024*1024 {
		return fmt.Sprintf("%.1fG", float64(b)/(1024*1024*1024))
	} else if b >= 1024*1024 {
		return fmt.Sprintf("%.1fM", float64(b)/(1024*1024))
	} else if b >= 1024 {
		return fmt.Sprintf("%.1fK", float64(b)/1024)
	}
	return fmt.Sprintf("%dB", b)
}

func getFileSize(path string) (int64, error) {
	var st syscall.Stat_t
	err := syscall.Stat(path, &st)
	if err != nil {
		return 0, err
	}
	return st.Size, nil
}

func fileExists(path string) bool {
	var st syscall.Stat_t
	return syscall.Stat(path, &st) == nil
}

func getFileBirthTime(path string) int64 {
	var st syscall.Stat_t
	err := syscall.Stat(path, &st)
	if err != nil {
		return 0
	}
	return st.Ctim.Sec
}

// logicalResolve finds all files referencing a physical extent via LOGICAL_INO_V2 + INO_LOOKUP.
// Returns absolute paths. mountFd must be an open fd to the btrfs mount point.
// subvolCache maps root ID → subvol path (avoids repeated subprocess calls).
func logicalResolve(mountFd int, mount string, physAddr uint64, subvolCache map[uint64]string) []string {
	le := binary.LittleEndian

	// Allocate buffer: logical_ino_args (56 bytes) header + data_container + space for results
	// We put the data_container right after the header in the same buffer
	const headerSize = 56
	const containerOff = 16 // inodes pointer offset in logical_ino_args
	bufSize := headerSize + 65536 // 64KB for results
	buf := make([]byte, bufSize)

	// Fill logical_ino_args
	le.PutUint64(buf[0:8], physAddr)                  // logical
	le.PutUint64(buf[8:16], uint64(bufSize-headerSize)) // size (of data container)
	// reserved[3] = 0 (already zeroed)
	le.PutUint64(buf[32:40], 0) // flags (0 for now, could use IGNORE_OFFSET)
	// inodes pointer = address of data container (right after header)
	le.PutUint64(buf[40:48], uint64(uintptr(unsafe.Pointer(&buf[headerSize]))))

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd),
		IOC_LOGICAL_INO_V2, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return nil
	}

	// Parse data_container at buf[headerSize:]
	dc := buf[headerSize:]
	elemCnt := le.Uint32(dc[8:12])

	var paths []string
	// Each element is a triplet: (inum uint64, offset uint64, root uint64)
	for i := uint32(0); i < elemCnt; i++ {
		off := 16 + i*24 // 16 = data_container header, 24 = 3×uint64
		if int(off+24) > len(dc) {
			break
		}
		inum := le.Uint64(dc[off : off+8])
		root := le.Uint64(dc[off+16 : off+24])

		// INO_LOOKUP: resolve inum+root to path
		var lookupBuf [4096]byte
		le.PutUint64(lookupBuf[0:8], root)  // treeid
		le.PutUint64(lookupBuf[8:16], inum) // objectid
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd),
			IOC_INO_LOOKUP, uintptr(unsafe.Pointer(&lookupBuf[0])))
		if errno != 0 {
			continue
		}
		// Name starts at offset 16, null-terminated
		name := ""
		for j := 16; j < len(lookupBuf); j++ {
			if lookupBuf[j] == 0 {
				name = string(lookupBuf[16:j])
				break
			}
		}
		if name == "" {
			continue
		}

		// Resolve the subvol path for this root (cached)
		subvolPath, ok := subvolCache[root]
		if !ok {
			subvolPath = resolveSubvolPath(mountFd, mount, root)
			subvolCache[root] = subvolPath
		}
		if subvolPath == "" {
			continue
		}
		fullPath := mount + "/" + subvolPath + "/" + name
		if fileExists(fullPath) {
			paths = append(paths, fullPath)
		}
	}
	return paths
}

// dedupGroup represents a set of identical files to deduplicate
type dedupGroup struct {
	paths          []string // first path is src, rest are dests
	estimatedSaved int64    // sum of unique extent sizes (physical, from FIEMAP)
	numCopies      int      // total copies in this group (len(paths))
	fileSize       int64    // size of the source file (for concurrency category)
	relPath        string   // relative path for in-flight tracking
}

// sizeCategory returns the concurrency category for a file size.
// Category 0 (<10MB): unlimited concurrency
// Category 1 (10-100MB): max 1 concurrent
// Category 2 (100MB-1GB): max 1 concurrent
// Category 3 (>1GB): max 1 concurrent
func sizeCategory(size int64) int {
	switch {
	case size < 10*1024*1024:
		return 0
	case size < 100*1024*1024:
		return 1
	case size < 1024*1024*1024:
		return 2
	default:
		return 3
	}
}

// debugTimedFn is a function type for debug timing. nil = no debug.
type debugTimedFn func(start time.Time, format string, args ...interface{}) time.Duration

// prefetchIntoCache reads the file into page cache via pread into a dummy buffer.
// btrfs may ignore fadvise/readahead (low ioprio), so brute-force pread is needed.
// If stop channel is provided and closed, aborts mid-read.
func prefetchIntoCache(fd int, size int64, stop <-chan struct{}) {
	const chunkSize = 128 * 1024 // 128KB per pread
	dummy := make([]byte, chunkSize)
	var off int64
	for off < size {
		if stop != nil {
			select {
			case <-stop:
				return
			default:
			}
		}
		n := size - off
		if n > chunkSize {
			n = chunkSize
		}
		syscall.Pread(fd, dummy[:n], off)
		off += n
	}
}

// dedupSingle calls FIDEDUPERANGE for a single dest against an open src fd.
// Returns (1 if deduped, bytes_deduped from kernel, error).
func dedupSingle(srcFd int, srcSize int64, dstPath string, dbg debugTimedFn) (int, int64, error) {
	t := time.Now()
	dstFd, err := syscall.Open(dstPath, syscall.O_RDONLY, 0)
	if dbg != nil {
		dbg(t, "open dst %s", dstPath)
	}
	if err != nil {
		return 0, 0, err
	}
	defer syscall.Close(dstFd)

	buf := make([]byte, DEDUPE_RANGE_SIZE+DEDUPE_RANGE_INFO_SIZE)
	le := binary.LittleEndian

	le.PutUint64(buf[0:8], 0)                    // src_offset
	le.PutUint64(buf[8:16], uint64(srcSize))      // src_length
	le.PutUint16(buf[16:18], 1)                   // dest_count = 1

	off := DEDUPE_RANGE_SIZE
	le.PutUint64(buf[off:off+8], uint64(dstFd))   // dest_fd
	le.PutUint64(buf[off+8:off+16], 0)             // dest_offset
	le.PutUint64(buf[off+16:off+24], 0)            // bytes_deduped (out)
	le.PutUint32(buf[off+24:off+28], 0)            // status (out)

	t = time.Now()
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(srcFd),
		IOC_FIDEDUPERANGE, uintptr(unsafe.Pointer(&buf[0])))
	if dbg != nil {
		dbg(t, "[%s] ioctl FIDEDUPERANGE dst=%s", fmtBytesStatic(srcSize), dstPath)
	}

	if errno != 0 {
		return 0, 0, errno
	}

	status := int32(le.Uint32(buf[off+24 : off+28]))
	bytesDeduped := int64(le.Uint64(buf[off+16 : off+24]))
	if status == 0 && bytesDeduped > 0 {
		return 1, bytesDeduped, nil
	}
	return 0, 0, nil
}

// fideduperange deduplicates src against dsts using single-dest calls with readahead.
// Dests sorted by phys addr. Blacklist skips phys groups with different content.
// Returns (total deduped dests, total actual bytes saved).
func fideduperange(srcPath string, dstPaths []string, fileSize int64, dbg debugTimedFn) (int, int64, error) {
	t := time.Now()
	srcFd, err := syscall.Open(srcPath, syscall.O_RDONLY, 0)
	if dbg != nil {
		dbg(t, "open src %s", srcPath)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("open src %s: %w", srcPath, err)
	}
	defer syscall.Close(srcFd)

	srcSize, err := getFileSize(srcPath)
	if err != nil {
		return 0, 0, err
	}

	if len(dstPaths) == 0 {
		return 0, 0, nil
	}

	// Prefetch source into page cache before starting any dedup.
	// Background goroutine then re-reads src + current dst every 5s to keep warm.
	t = time.Now()
	prefetchIntoCache(srcFd, srcSize, nil)
	if dbg != nil {
		dbg(t, "[%s] prefetch src %s", fmtBytesStatic(srcSize), srcPath)
	}

	stopRefresh := make(chan struct{})
	var currentDstPath atomic.Value // stores string
	go func() {
		for {
			select {
			case <-stopRefresh:
				return
			case <-time.After(10 * time.Second):
			}
			t0 := time.Now()
			prefetchIntoCache(srcFd, srcSize, stopRefresh)
			if dbg != nil {
				dbg(t0, "[%s] prefetch src (refresh) %s", fmtBytesStatic(srcSize), srcPath)
			}
			if dp, ok := currentDstPath.Load().(string); ok && dp != "" {
				t1 := time.Now()
				dfd, err := syscall.Open(dp, syscall.O_RDONLY, 0)
				if err == nil {
					prefetchIntoCache(dfd, srcSize, stopRefresh)
					syscall.Close(dfd)
					if dbg != nil {
						dbg(t1, "[%s] prefetch dst (refresh) %s", fmtBytesStatic(srcSize), dp)
					}
				}
			}
		}
	}()

	// Get phys addr for each dest and sort by it
	type destEntry struct {
		path string
		phys uint64
	}
	dests := make([]destEntry, 0, len(dstPaths))
	for _, dp := range dstPaths {
		phys, _, err := getFirstExtentPhys(dp)
		if err != nil {
			phys = 0 // unknown — will be processed, not skipped
		}
		dests = append(dests, destEntry{path: dp, phys: phys})
	}
	sort.Slice(dests, func(i, j int) bool { return dests[i].phys < dests[j].phys })

	// Single-dest dedup loop with phys-addr blacklist
	blacklist := map[uint64]bool{}
	savedPerPhys := map[uint64]bool{} // track which phys addrs already counted for saved
	totalDeduped := 0
	var totalSaved int64

	for _, d := range dests {
		if d.phys != 0 && blacklist[d.phys] {
			continue // content differs for this phys addr — skip
		}

		// Prefetch dest into page cache via pread (brute force)
		tRA := time.Now()
		dstFd, err := syscall.Open(d.path, syscall.O_RDONLY, 0)
		if err == nil {
			prefetchIntoCache(dstFd, srcSize, nil)
			syscall.Close(dstFd)
			if dbg != nil {
				dbg(tRA, "[%s] prefetch dst %s", fmtBytesStatic(srcSize), d.path)
			}
		}

		currentDstPath.Store(d.path)
		n, bytesDeduped, err := dedupSingle(srcFd, srcSize, d.path, dbg)
		if err != nil {
			continue
		}

		if n == 0 {
			// Content differs — blacklist this phys addr
			if d.phys != 0 {
				blacklist[d.phys] = true
				if dbg != nil {
					dbg(time.Now(), "blacklist phys=0x%x (content differs)", d.phys)
				}
			}
			continue
		}

		totalDeduped++
		// Count saved only once per unique phys addr
		if d.phys == 0 || !savedPerPhys[d.phys] {
			totalSaved += bytesDeduped
			if d.phys != 0 {
				savedPerPhys[d.phys] = true
			}
		}
	}

	close(stopRefresh)
	return totalDeduped, totalSaved, nil
}

type snapshotInfo struct {
	subvolID     uint64
	relPath      string // path relative to mount
	creationTime int64  // epoch seconds
}

// subvolFullInfo carries everything we need for nested-subvol-aware snapshot lookup.
type subvolFullInfo struct {
	subvolID     uint64
	uuid         [16]byte
	parentUUID   [16]byte // zero for non-snapshot subvols
	relPath      string   // resolved path relative to mount; may be "" if resolution failed
	creationTime int64
}

// snapEntry is one snapshot of a particular live subvol, sorted oldest-first per group.
type snapEntry struct {
	absPath      string
	creationTime int64
}

// liveSubvolEntry: one subvol within the live tree, with its snapshots
// (= subvols whose parent_uuid matches this subvol's uuid).
// relPath is the path of THIS subvol relative to the mount.
type liveSubvolEntry struct {
	relPath   string // e.g. "BackupComputer/BlackTower"
	rootID    uint64
	snapshots []snapEntry // oldest first
}

// subvolMap holds liveSubvolEntries sorted by relPath length DESC for longest-prefix match.
type subvolMap []liveSubvolEntry

// findForRel locates the liveSubvolEntry that owns this rel-path (relative to mount).
// Returns the entry and the path within that subvol (with leading slash stripped).
func (m subvolMap) findForRel(rel string) (*liveSubvolEntry, string) {
	for i := range m {
		p := m[i].relPath
		if rel == p {
			return &m[i], ""
		}
		if strings.HasPrefix(rel, p+"/") {
			return &m[i], rel[len(p)+1:]
		}
	}
	return nil, rel
}

// uuidToString formats a 16-byte UUID as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
func uuidToString(b []byte) string {
	if len(b) < 16 {
		return ""
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.LittleEndian.Uint32(b[0:4]),
		binary.LittleEndian.Uint16(b[4:6]),
		binary.LittleEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16])
}

func uuidIsZero(b []byte) bool {
	for _, v := range b[:16] {
		if v != 0 {
			return false
		}
	}
	return true
}

// getSubvolUUID returns the UUID of a subvolume via GET_SUBVOL_INFO ioctl
func getSubvolUUID(path string) ([16]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return [16]byte{}, err
	}
	defer syscall.Close(fd)
	info, err := getSubvolInfo(fd)
	if err != nil {
		return [16]byte{}, err
	}
	var uuid [16]byte
	for i := 0; i < 16; i++ {
		uuid[i] = byte(info.uuid[i])
	}
	return uuid, nil
}

// getSubvolSnapshots finds all snapshots by scanning root tree for subvol IDs.
// Parses parent_uuid directly from root_item in tree search (no subprocess per subvol).
// Only resolves paths for matching snapshots.
func getSubvolSnapshots(mountFd int, mount string, targetUUID [16]byte) ([]snapshotInfo, error) {
	le := binary.LittleEndian
	var snapshots []snapshotInfo

	// Offsets within btrfs_root_item (from kernel headers)
	const rootItemParentUUIDOff = 263
	const rootItemOtimeOff = 339 // btrfs_timespec: __le64 sec + __le32 nsec
	const rootItemMinSize = 353  // need at least up to otime.sec

	// Scan root tree, parse parent_uuid inline — only resolve path for matches
	type candidate struct {
		subvolID uint64
		otime    int64
	}
	var candidates []candidate
	var legacyIDs []uint64
	seen := make(map[uint64]bool)

	var minObjID uint64 = 256
	for {
		buf := make([]byte, SEARCH_KEY_SIZE+8+65536)
		le.PutUint64(buf[0:8], 1) // root tree
		le.PutUint64(buf[8:16], minObjID)
		le.PutUint64(buf[16:24], ^uint64(0))
		le.PutUint64(buf[24:32], 0)
		le.PutUint64(buf[32:40], ^uint64(0))
		le.PutUint64(buf[40:48], 0)
		le.PutUint64(buf[48:56], ^uint64(0))
		le.PutUint32(buf[56:60], BTRFS_ROOT_ITEM_KEY)
		le.PutUint32(buf[60:64], BTRFS_ROOT_ITEM_KEY)
		le.PutUint32(buf[64:68], ^uint32(0))
		le.PutUint64(buf[SEARCH_KEY_SIZE:SEARCH_KEY_SIZE+8], 65536)

		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd),
			IOC_TREE_SEARCH_V2, uintptr(unsafe.Pointer(&buf[0])))
		if errno != 0 {
			break
		}

		nrItems := le.Uint32(buf[64:68])
		if nrItems == 0 {
			break
		}

		pos := SEARCH_KEY_SIZE + 8
		var lastObjID uint64
		for i := uint32(0); i < nrItems; i++ {
			objID := le.Uint64(buf[pos+8 : pos+16])
			hdrLen := le.Uint32(buf[pos+28 : pos+32])
			pos += SEARCH_HEADER_SIZE
			lastObjID = objID

			// Parse parent_uuid directly from root_item data (skip duplicates)
			if !seen[objID] {
				seen[objID] = true
				if int(hdrLen) >= rootItemMinSize {
					data := buf[pos : pos+int(hdrLen)]
					var parentUUID [16]byte
					copy(parentUUID[:], data[rootItemParentUUIDOff:rootItemParentUUIDOff+16])
					if parentUUID == targetUUID {
						otimeSec := int64(le.Uint64(data[rootItemOtimeOff : rootItemOtimeOff+8]))
						candidates = append(candidates, candidate{subvolID: objID, otime: otimeSec})
					}
				} else {
					// Legacy root_item (< 439 bytes) — no UUID fields inline.
					// Fall back to GET_SUBVOL_INFO ioctl.
					legacyIDs = append(legacyIDs, objID)
				}
			}

			pos += int(hdrLen)
		}
		minObjID = lastObjID + 1
		if minObjID == 0 {
			break
		}
	}

	// Legacy root_items: check via GET_SUBVOL_INFO (slower, needs path resolve)
	for _, svID := range legacyIDs {
		relPath := resolveSubvolPath(mountFd, mount, svID)
		if relPath == "" {
			continue
		}
		absPath := mount + "/" + relPath
		fd, err := syscall.Open(absPath, syscall.O_RDONLY, 0)
		if err != nil {
			continue
		}
		info, err := getSubvolInfo(fd)
		syscall.Close(fd)
		if err != nil {
			continue
		}
		var parentUUID [16]byte
		for k := 0; k < 16; k++ {
			parentUUID[k] = byte(info.parent_uuid[k])
		}
		if parentUUID == targetUUID {
			otimeSec := int64(info.otime.sec)
			candidates = append(candidates, candidate{subvolID: svID, otime: otimeSec})
		}
	}

	// Only resolve paths for matching snapshots
	for _, c := range candidates {
		relPath := resolveSubvolPath(mountFd, mount, c.subvolID)
		if relPath == "" {
			continue
		}
		snapshots = append(snapshots, snapshotInfo{
			subvolID:     c.subvolID,
			relPath:      relPath,
			creationTime: c.otime,
		})
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].creationTime < snapshots[j].creationTime
	})
	return snapshots, nil
}

// enumerateAllSubvols scans the root tree once and returns metadata for every
// subvolume on the filesystem (including nested subvols and snapshots).
// Path resolution is deferred to the caller (only resolve what's needed).
func enumerateAllSubvols(mountFd int, mount string) ([]subvolFullInfo, error) {
	le := binary.LittleEndian
	var result []subvolFullInfo

	const rootItemUUIDOff = 247       // btrfs_root_item.uuid offset
	const rootItemParentUUIDOff = 263 // btrfs_root_item.parent_uuid offset
	const rootItemOtimeOff = 339      // btrfs_timespec sec
	const rootItemMinSize = 353       // need fields up through otime.sec

	type basic struct {
		subvolID   uint64
		uuid       [16]byte
		parentUUID [16]byte
		otime      int64
		legacy     bool // true → root_item too small, fall back to GET_SUBVOL_INFO
	}
	var entries []basic
	seen := make(map[uint64]bool)

	var minObjID uint64 = 256
	for {
		buf := make([]byte, SEARCH_KEY_SIZE+8+65536)
		le.PutUint64(buf[0:8], 1) // root tree
		le.PutUint64(buf[8:16], minObjID)
		le.PutUint64(buf[16:24], ^uint64(0))
		le.PutUint64(buf[24:32], 0)
		le.PutUint64(buf[32:40], ^uint64(0))
		le.PutUint64(buf[40:48], 0)
		le.PutUint64(buf[48:56], ^uint64(0))
		le.PutUint32(buf[56:60], BTRFS_ROOT_ITEM_KEY)
		le.PutUint32(buf[60:64], BTRFS_ROOT_ITEM_KEY)
		le.PutUint32(buf[64:68], ^uint32(0))
		le.PutUint64(buf[SEARCH_KEY_SIZE:SEARCH_KEY_SIZE+8], 65536)

		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd),
			IOC_TREE_SEARCH_V2, uintptr(unsafe.Pointer(&buf[0])))
		if errno != 0 {
			break
		}

		nrItems := le.Uint32(buf[64:68])
		if nrItems == 0 {
			break
		}

		pos := SEARCH_KEY_SIZE + 8
		var lastObjID uint64
		for i := uint32(0); i < nrItems; i++ {
			objID := le.Uint64(buf[pos+8 : pos+16])
			hdrLen := le.Uint32(buf[pos+28 : pos+32])
			pos += SEARCH_HEADER_SIZE
			lastObjID = objID

			if !seen[objID] {
				seen[objID] = true
				if int(hdrLen) >= rootItemMinSize {
					data := buf[pos : pos+int(hdrLen)]
					var b basic
					b.subvolID = objID
					copy(b.uuid[:], data[rootItemUUIDOff:rootItemUUIDOff+16])
					copy(b.parentUUID[:], data[rootItemParentUUIDOff:rootItemParentUUIDOff+16])
					b.otime = int64(le.Uint64(data[rootItemOtimeOff : rootItemOtimeOff+8]))
					entries = append(entries, b)
				} else {
					entries = append(entries, basic{subvolID: objID, legacy: true})
				}
			}
			pos += int(hdrLen)
		}
		minObjID = lastObjID + 1
		if minObjID == 0 {
			break
		}
	}

	// Resolve legacy entries via GET_SUBVOL_INFO (need a path first)
	for i, e := range entries {
		if !e.legacy {
			continue
		}
		relPath := resolveSubvolPath(mountFd, mount, e.subvolID)
		if relPath == "" {
			continue
		}
		fd, err := syscall.Open(mount+"/"+relPath, syscall.O_RDONLY, 0)
		if err != nil {
			continue
		}
		info, err := getSubvolInfo(fd)
		syscall.Close(fd)
		if err != nil {
			continue
		}
		for k := 0; k < 16; k++ {
			entries[i].uuid[k] = byte(info.uuid[k])
			entries[i].parentUUID[k] = byte(info.parent_uuid[k])
		}
		entries[i].otime = int64(info.otime.sec)
	}

	// Resolve paths for all (skip if subprocess fails — those entries get relPath="")
	for _, e := range entries {
		relPath := resolveSubvolPath(mountFd, mount, e.subvolID)
		result = append(result, subvolFullInfo{
			subvolID:     e.subvolID,
			uuid:         e.uuid,
			parentUUID:   e.parentUUID,
			relPath:      relPath,
			creationTime: e.otime,
		})
	}
	return result, nil
}

// resolveSubvolPath resolves a subvolume ID to its path relative to the mount
// Uses BTRFS_IOC_INO_LOOKUP with treeid=subvolID, objectid=256 (FIRST_FREE_OBJECTID)
// then walks up via ROOT_BACKREF to build the full path.
func resolveSubvolPath(fd int, mount string, subvolID uint64) string {
	// Use btrfs inspect-internal subvolid-resolve as fallback
	// This is simpler than walking ROOT_BACKREF manually
	out, err := exec.Command("btrfs", "inspect-internal", "subvolid-resolve",
		fmt.Sprintf("%d", subvolID), mount).Output()
	if err != nil {
		return ""
	}
	p := strings.TrimSpace(string(out))
	p = strings.TrimRight(p, "/")
	return p
}

// getSubvolID gets the subvolume ID for a path via INO_LOOKUP
func getSubvolID(path string) (uint64, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)

	// BTRFS_IOC_INO_LOOKUP with objectid=256 returns treeid=subvol_id
	var args [4096]byte
	le := binary.LittleEndian
	le.PutUint64(args[0:8], 0)   // treeid (0 = auto)
	le.PutUint64(args[8:16], 256) // objectid = FIRST_FREE_OBJECTID

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		IOC_INO_LOOKUP, uintptr(unsafe.Pointer(&args[0])))
	if errno != 0 {
		return 0, errno
	}

	treeid := le.Uint64(args[0:8])
	return treeid, nil
}

type counters struct {
	checked       int
	candidates    int
	copies        int
	notFound      int
	changed       int
	shared        int
	deduped       atomic.Int64
	dedupedCopies atomic.Int64
	pending       atomic.Int64
	pendingCopies atomic.Int64
	pendingSaved  atomic.Int64
	bytesSaved    atomic.Int64

	// Active dedup tracking: file sizes currently being deduped
	activeMu    sync.Mutex
	activeSizes []int64 // sorted big→small on read

	// Source selection optimization counters
	optCompressed    int // source chosen because compressed
	optLessFragmented int // source chosen because less fragmented
	optLessRewrites      int // source chosen because more refs (tiebreaker)
	optDefault       int // default (live file as source)

	probeSkipped atomic.Int64 // groups skipped because probe dedup had 0 gain
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "btrfs-snapshot-dedup v%s\n\n", VERSION)
	fmt.Fprintf(os.Stderr, "Usage: %s [options] <mount> <subvol> [find-filter...]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Options:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nEverything after <mount> <subvol> is passed to find(1) as filter.\n")
	fmt.Fprintf(os.Stderr, "Default (no filter): find <path> -type f\n\n")
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  # Dedup all files\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Only large files (>= 100MB)\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol -size +100M\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Only text/config files\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol '(' -iname '*.txt' -o -iname '*.json' ')'\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Combine size + extension filter\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol -size +1M '(' -iname '*.log' -o -iname '*.txt' ')'\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Resume after interrupt\n")
	fmt.Fprintf(os.Stderr, "  sudo %s -start-at 'path/to/last/file' /mnt/btrfs mysubvol\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Stay on the start subvol only — skip nested subvols (find -xdev)\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol -xdev\n", os.Args[0])
}

func main() {
	workers := flag.Int("workers", DEDUP_WORKERS, "number of parallel dedup workers")
	startAt := flag.String("start-at", "", "resume: skip files until this relative path (lexicographic)")
	debugMs := flag.Int("debug", 0, "enable debug log: log actions taking >= N ms (e.g. --debug=100)")
	writeCandidates := flag.Bool("write-candidates", false, "write candidates.fdupes and candidates.done files")
	flag.Usage = printUsage
	flag.Parse()

	// Debug logger: writes to debug.log, only actions >= debugMs
	var debugLog func(format string, args ...interface{})
	var debugFile *os.File
	if *debugMs > 0 {
		var err error
		debugFile, err = os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: could not open debug.log: %v\n", err)
			debugLog = func(format string, args ...interface{}) {} // noop
		} else {
			debugThreshold := time.Duration(*debugMs) * time.Millisecond
			_ = debugThreshold // used in debugTimed
			debugLog = func(format string, args ...interface{}) {
				fmt.Fprintf(debugFile, "[%s] %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
			}
			defer debugFile.Close()
			debugLog("debug started: threshold=%dms workers=%d", *debugMs, *workers)
		}
	} else {
		debugLog = func(format string, args ...interface{}) {} // noop
	}
	debugThresholdMs := int64(*debugMs)

	// debugTimed logs if elapsed >= threshold. Returns elapsed for chaining.
	debugTimed := func(start time.Time, format string, args ...interface{}) time.Duration {
		elapsed := time.Since(start)
		if debugThresholdMs > 0 && elapsed.Milliseconds() >= debugThresholdMs {
			msg := fmt.Sprintf(format, args...)
			fmt.Fprintf(debugFile, "%s [%s] %s\n", time.Now().Format("15:04:05.000"), elapsed.Round(time.Millisecond), msg)
		}
		return elapsed
	}

	// First two positional args are mount + subvol, rest goes to find(1)
	args := flag.Args()
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	if os.Geteuid() != 0 {
		fmt.Fprintf(os.Stderr, "ERROR: btrfs-snapshot-dedup requires root (uses BTRFS ioctls). Run with sudo.\n")
		os.Exit(1)
	}
	mount := args[0]
	subvol := args[1]
	findFilter := args[2:] // remaining args → find(1) filter

	mount = strings.TrimRight(mount, "/")
	subvol = strings.TrimRight(subvol, "/")
	live := mount + "/" + subvol

	if _, err := os.Stat(live); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s not found\n", live)
		os.Exit(1)
	}

	// Get subvolume ID + UUID via ioctl (stable kernel API)
	subvolID, err := getSubvolID(live)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot get subvol ID for %s: %v\n", live, err)
		os.Exit(1)
	}

	subvolUUID, err := getSubvolUUID(live)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot get UUID for %s: %v\n", live, err)
		os.Exit(1)
	}

	mountFd, err := syscall.Open(mount, syscall.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot open %s: %v\n", mount, err)
		os.Exit(1)
	}
	defer syscall.Close(mountFd)

	fmt.Fprintf(os.Stderr, "btrfs-snapshot-dedup v%s\n", VERSION)

	// Enumerate ALL subvols on the FS (one tree-search pass) so we can handle
	// nested subvols inside the live tree — their snapshots have a different
	// parent_uuid and live elsewhere on disk.
	fmt.Fprintf(os.Stderr, "Scanning subvolumes...")
	allSubvols, err := enumerateAllSubvols(mountFd, mount)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, " %d total.\n", len(allSubvols))

	// Index by uuid for parent_uuid → snapshot lookup
	uuidToSnaps := make(map[[16]byte][]snapEntry)
	subvolCache := make(map[uint64]string) // root ID → relPath (for logicalResolve)
	for _, sv := range allSubvols {
		if sv.relPath != "" {
			subvolCache[sv.subvolID] = sv.relPath
		}
		// non-snapshots have parent_uuid == 0
		var zero [16]byte
		if sv.parentUUID == zero {
			continue
		}
		if sv.relPath == "" {
			continue
		}
		uuidToSnaps[sv.parentUUID] = append(uuidToSnaps[sv.parentUUID], snapEntry{
			absPath:      mount + "/" + sv.relPath,
			creationTime: sv.creationTime,
		})
	}

	// Build the subvol-map for the live tree: start subvol + every subvol
	// nested under it. relPath comparison is against the subvol's path
	// relative to mount.
	var liveMap subvolMap
	totalSnaps := 0
	for _, sv := range allSubvols {
		if sv.relPath == "" {
			continue
		}
		// "subvol" is the start subvol's relPath relative to mount. Match exactly
		// or as prefix (with separator).
		if sv.relPath != subvol && !strings.HasPrefix(sv.relPath, subvol+"/") {
			continue
		}
		// non-snapshot subvols only — snapshots themselves shouldn't be dedup targets
		var zero [16]byte
		if sv.parentUUID != zero {
			continue
		}
		snaps := uuidToSnaps[sv.uuid]
		sort.Slice(snaps, func(i, j int) bool {
			return snaps[i].creationTime < snaps[j].creationTime
		})
		liveMap = append(liveMap, liveSubvolEntry{
			relPath:   sv.relPath,
			rootID:    sv.subvolID,
			snapshots: snaps,
		})
		totalSnaps += len(snaps)
	}
	if len(liveMap) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: no subvolumes found under %s\n", subvol)
		os.Exit(1)
	}
	if totalSnaps == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: no snapshots found for any subvol under %s\n", subvol)
		os.Exit(1)
	}

	// Sort by relPath length DESC so longest prefix wins in findForRel
	sort.Slice(liveMap, func(i, j int) bool {
		return len(liveMap[i].relPath) > len(liveMap[j].relPath)
	})

	// Verify snapshots are accessible — pick the first subvol with snapshots
	for _, e := range liveMap {
		if len(e.snapshots) > 0 {
			testSnap := e.snapshots[0].absPath
			if _, err := os.Stat(testSnap); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: snapshot not accessible: %s\n", testSnap)
				fmt.Fprintf(os.Stderr, "Snapshots are resolved relative to the btrfs top-level.\n")
				fmt.Fprintf(os.Stderr, "If you use subvolume mounts (e.g. subvol=@), mount with subvolid=5 instead:\n")
				fmt.Fprintf(os.Stderr, "  mount -o subvolid=5 /dev/... /mnt/toplevel\n")
				fmt.Fprintf(os.Stderr, "  %s /mnt/toplevel %s\n", os.Args[0], subvol)
				os.Exit(1)
			}
			break
		}
	}

	fmt.Fprintf(os.Stderr, "Mount:    %s\n", mount)
	fmt.Fprintf(os.Stderr, "Subvol:   %s (id=%d) + %d nested subvol(s)\n",
		subvol, subvolID, len(liveMap)-1)
	fmt.Fprintf(os.Stderr, "Snapshots: %d total across %d subvol(s)\n",
		totalSnaps, len(liveMap))
	for _, e := range liveMap {
		if len(e.snapshots) == 0 {
			fmt.Fprintf(os.Stderr, "  %s — (no snapshots)\n", e.relPath)
		} else {
			fmt.Fprintf(os.Stderr, "  %s — %d snapshots\n", e.relPath, len(e.snapshots))
		}
	}
	_ = subvolUUID
	if len(findFilter) > 0 {
		fmt.Fprintf(os.Stderr, "Filter:   %s\n", strings.Join(findFilter, " "))
	}
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")
	fmt.Fprintln(os.Stderr, "  found      = files matching size filter (walker progress)")
	fmt.Fprintln(os.Stderr, "  buf        = files in queue waiting to be processed")
	fmt.Fprintln(os.Stderr, "  checked    = processed/shared/not_found/changed")
	fmt.Fprintln(os.Stderr, "  pending    = groups/copies/expectedSavings(active dedup sizes) waiting for dedup")
	fmt.Fprintln(os.Stderr, "  deduped    = groups/copies/saved")
	fmt.Fprintln(os.Stderr, "  skip       = groups skipped (probe: already same extent, file >= 1MB)")
	fmt.Fprintln(os.Stderr, "  opt        = compressed/lessFragmented/lessRewrites/default (source selection)")
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")

	// Binary search: find oldest snapshot containing a file
	// snaps is the per-subvol snapshot list (oldest-first); subRel is the
	// path within that subvol.
	findOldestSnap := func(snaps []snapEntry, subRel string) int {
		n := len(snaps)
		if n == 0 {
			return -1
		}
		hi := n - 1
		if !fileExists(snaps[hi].absPath + "/" + subRel) {
			return -1
		}
		if fileExists(snaps[0].absPath + "/" + subRel) {
			return 0
		}
		lo := 0
		result := -1
		for lo <= hi {
			mid := (lo + hi) / 2
			if fileExists(snaps[mid].absPath + "/" + subRel) {
				result = mid
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		return result
	}

	// File discovery via find(1)
	fileQ := NewSpillQueue(QUEUE_LIMIT)
	walkDone := make(chan bool, 1)
	var findDone atomic.Bool

	skipping := *startAt != ""
	if skipping {
		fmt.Fprintf(os.Stderr, "Resume: skipping until %s\n", *startAt)
	}

	go func() {
		findArgs := []string{live, "-type", "f"}
		if len(findFilter) > 0 {
			findArgs = append(findArgs, "(")
			for _, arg := range findFilter {
				findArgs = append(findArgs, strings.Fields(arg)...)
			}
			findArgs = append(findArgs, ")")
		}
		fmt.Fprintf(os.Stderr, "find %s\n", strings.Join(findArgs, " "))

		cmd := exec.Command("find", findArgs...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: find: %v\n", err)
			fileQ.Close()
			findDone.Store(true)
			walkDone <- true
			return
		}
		cmd.Start()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			path := scanner.Text()
			if skipping {
				rel := strings.TrimPrefix(path, live+"/")
				if rel < *startAt {
					continue
				}
				skipping = false
				fmt.Fprintf(os.Stderr, "Resume: starting at %s\n", rel)
			}
			fileQ.Push(path)
		}
		cmd.Wait()
		fileQ.Close()
		findDone.Store(true)
		walkDone <- true
	}()

	// Status — independent goroutine
	var cnt counters
	startTime := time.Now()
	statusDone := make(chan bool)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		time.Sleep(1 * time.Second)
		for {
			elapsed := int(time.Since(startTime).Seconds())
			etaStr := ""
			totalFound := fileQ.Total()
			checkedStr := fmt.Sprintf("%d", cnt.checked)
			if findDone.Load() && totalFound > 0 && cnt.checked > 0 {
				pending := cnt.pending.Load()
				deduped := cnt.deduped.Load()
				if int64(cnt.checked) >= totalFound && pending > 0 && deduped > 0 {
					// Scan done, dedup phase — ETA based on dedup rate
					dedupRate := float64(deduped) / float64(elapsed)
					if dedupRate > 0 {
						dedupETA := int(float64(pending) / dedupRate)
						etaStr = fmt.Sprintf(" scanned, deduping ETA:%s", fmtTime(dedupETA))
					}
				} else if int64(cnt.checked) < totalFound {
					// Scan phase — ETA based on scan rate
					pct := int64(cnt.checked) * 100 / totalFound
					remaining := int(int64(totalFound-int64(cnt.checked)) * int64(elapsed) / int64(cnt.checked))
					etaStr = fmt.Sprintf(" %d%% ETA:%s", pct, fmtTime(remaining))
				} else {
					etaStr = " done"
				}
			}
			_ = fileQ.Buffered() // keep queue active
			fmtBytes := func(b int64) string {
				if b >= 1024*1024*1024 {
					return fmt.Sprintf("%.1fG", float64(b)/(1024*1024*1024))
				} else if b >= 1024*1024 {
					return fmt.Sprintf("%.1fM", float64(b)/(1024*1024))
				} else if b >= 1024 {
					return fmt.Sprintf("%.1fK", float64(b)/1024)
				}
				return fmt.Sprintf("%dB", b)
			}
			saved := cnt.bytesSaved.Load()
			pendingSaved := cnt.pendingSaved.Load()
			// Build active sizes string (sorted big→small)
			cnt.activeMu.Lock()
			activeStr := ""
			if len(cnt.activeSizes) > 0 {
				sorted := make([]int64, len(cnt.activeSizes))
				copy(sorted, cnt.activeSizes)
				sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
				parts := make([]string, len(sorted))
				for i, s := range sorted {
					parts[i] = fmtBytes(s)
				}
				activeStr = strings.Join(parts, ",")
			}
			cnt.activeMu.Unlock()
			fmt.Fprintf(os.Stderr, "  [%s] found=%d checked=%s/%d/%d/%d pending=%d/%d/%s(%s) deduped=%d/%d/%s skip=%d opt=%d/%d/%d/%d%s\n",
				fmtTime(elapsed), totalFound, checkedStr, cnt.shared, cnt.notFound, cnt.changed,
				cnt.pending.Load(), cnt.pendingCopies.Load(), fmtBytes(pendingSaved), activeStr,
				cnt.deduped.Load(), cnt.dedupedCopies.Load(), fmtBytes(saved),
				cnt.probeSkipped.Load(),
				cnt.optCompressed, cnt.optLessFragmented, cnt.optLessRewrites, cnt.optDefault, etaStr)

			select {
			case <-statusDone:
				return
			case <-ticker.C:
			}
		}
	}()

	// Output files
	logFile, _ := os.OpenFile("bees-snapshot-dedup.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer logFile.Close()

	// Optional candidate files (--write-candidates)
	var fdupesFile, doneFile *os.File
	if *writeCandidates {
		fdupesFile, _ = os.OpenFile("candidates.fdupes", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer fdupesFile.Close()
		doneFile, _ = os.OpenFile("candidates.done", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer doneFile.Close()
	}

	var fdupesMu sync.Mutex // protects fdupesFile writes
	var doneMu sync.Mutex   // protects doneFile writes

	writeFdupesGroup := func(paths []string) {
		if fdupesFile == nil {
			return
		}
		t := time.Now()
		fdupesMu.Lock()
		for _, p := range paths {
			fmt.Fprintln(fdupesFile, p)
		}
		fmt.Fprintln(fdupesFile)
		fdupesFile.Sync()
		fdupesMu.Unlock()
		debugTimed(t, "writeFdupes %d paths", len(paths))
	}

	writeDoneGroup := func(paths []string) {
		if doneFile == nil {
			return
		}
		t := time.Now()
		doneMu.Lock()
		for _, p := range paths {
			fmt.Fprintln(doneFile, p)
		}
		fmt.Fprintln(doneFile)
		doneFile.Sync()
		doneMu.Unlock()
		debugTimed(t, "writeDone %d paths", len(paths))
	}

	// In-flight tracker: tracks oldest pending dedup path for correct resume point
	var inflightMu sync.Mutex
	var inflightQueue []string // ordered by scan order (append-only, remove from front)

	inflightAdd := func(path string) {
		inflightMu.Lock()
		inflightQueue = append(inflightQueue, path)
		inflightMu.Unlock()
	}

	inflightRemove := func(path string) {
		t := time.Now()
		inflightMu.Lock()
		for i, p := range inflightQueue {
			if p == path {
				inflightQueue = append(inflightQueue[:i], inflightQueue[i+1:]...)
				break
			}
		}
		n := len(inflightQueue)
		inflightMu.Unlock()
		debugTimed(t, "inflightRemove (queue=%d)", n)
	}

	inflightOldest := func() string {
		inflightMu.Lock()
		defer inflightMu.Unlock()
		if len(inflightQueue) > 0 {
			return inflightQueue[0]
		}
		return ""
	}

	// Dedup worker pool with per-category concurrency limits
	// 4 queues by size category, workers pick from large→small, fallback to cat0
	// Category 0 (<10MB): unlimited concurrency
	// Category 1-3 (>=10MB): max 1 concurrent per category (semaphore)
	dedupCh := make(chan dedupGroup, 10000)
	var dedupWg sync.WaitGroup

	// Per-category queues (slice behind mutex)
	type catQueue struct {
		mu    sync.Mutex
		items []dedupGroup
	}
	catQ := [4]*catQueue{{}, {}, {}, {}}
	for i := range catQ {
		catQ[i] = &catQueue{}
	}

	// Semaphores for categories 1-3 (tryAcquire/release)
	catSem := [3]atomic.Bool{} // false = free, true = held

	// Notify channel: signaled when new items are enqueued
	notify := make(chan struct{}, 1)

	notifyWorkers := func() {
		select {
		case notify <- struct{}{}:
		default:
		}
	}

	// Worker function
	doDedup := func(group dedupGroup) {
		dedupStart := time.Now()
		cnt.activeMu.Lock()
		cnt.activeSizes = append(cnt.activeSizes, group.fileSize)
		cnt.activeMu.Unlock()
		src := group.paths[0]
		dsts := group.paths[1:]

		var dbg debugTimedFn
		if debugThresholdMs > 0 {
			dbg = debugTimed
		}

		n, savedBytes, err := fideduperange(src, dsts, group.fileSize, dbg)
		debugTimed(dedupStart, "[%s, %d dests] fideduperange %s", fmtBytesStatic(group.fileSize), len(dsts), src)
		if err != nil && n == 0 {
			fmt.Fprintf(logFile, "dedup error: %s: %v\n", src, err)
		}

		if n > 0 {
			cnt.bytesSaved.Add(savedBytes)
			writeDoneGroup(group.paths)
		} else if n == 0 && len(dsts) > 0 {
			cnt.probeSkipped.Add(1)
		}
		cnt.deduped.Add(1)
		cnt.dedupedCopies.Add(int64(group.numCopies))
		cnt.pending.Add(-1)
		cnt.pendingCopies.Add(-int64(group.numCopies))
		cnt.pendingSaved.Add(-group.estimatedSaved)
		cnt.activeMu.Lock()
		for i, s := range cnt.activeSizes {
			if s == group.fileSize {
				cnt.activeSizes = append(cnt.activeSizes[:i], cnt.activeSizes[i+1:]...)
				break
			}
		}
		cnt.activeMu.Unlock()
		inflightRemove(group.relPath)
	}

	// tryPop from a category queue
	tryPop := func(cat int) (dedupGroup, bool) {
		q := catQ[cat]
		q.mu.Lock()
		defer q.mu.Unlock()
		if len(q.items) == 0 {
			return dedupGroup{}, false
		}
		g := q.items[0]
		q.items = q.items[1:]
		return g, true
	}

	// Worker: pick from large→small, cat0 as fallback
	var workersDone atomic.Bool

	for i := 0; i < *workers; i++ {
		dedupWg.Add(1)
		go func() {
			defer dedupWg.Done()
			for {
				var picked bool
				// Try categories 3→1 (large first): only if semaphore free
				for c := 3; c >= 1; c-- {
					if catSem[c-1].CompareAndSwap(false, true) {
						if g, ok := tryPop(c); ok {
							doDedup(g)
							catSem[c-1].Store(false)
							notifyWorkers() // wake others, semaphore freed
							picked = true
							break
						}
						catSem[c-1].Store(false) // nothing in queue, release
					}
				}
				if picked {
					continue
				}

				// Fallback: cat0 (unlimited, no semaphore)
				if g, ok := tryPop(0); ok {
					doDedup(g)
					continue
				}

				// Nothing available — wait for notification or exit
				if workersDone.Load() {
					// Check once more before exiting
					empty := true
					for c := 0; c < 4; c++ {
						catQ[c].mu.Lock()
						if len(catQ[c].items) > 0 {
							empty = false
						}
						catQ[c].mu.Unlock()
					}
					if empty {
						return
					}
					continue
				}
				select {
				case <-notify:
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()
	}

	// Dispatcher: reads from dedupCh, routes to category queues
	dedupWg.Add(1)
	go func() {
		defer dedupWg.Done()
		for group := range dedupCh {
			cat := sizeCategory(group.fileSize)
			q := catQ[cat]
			q.mu.Lock()
			q.items = append(q.items, group)
			q.mu.Unlock()
			notifyWorkers()
		}
		workersDone.Store(true)
		// Wake all workers so they can exit
		for i := 0; i < *workers; i++ {
			notifyWorkers()
		}
	}()

	// Handle SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	var interrupted atomic.Bool
	go func() {
		<-sigCh
		interrupted.Store(true)
		fmt.Fprintln(os.Stderr, "\nInterrupted! Finishing current dedup workers...")
	}()

	var lastFile string

	processFile := func(file string) {
		cnt.checked++
		// rel is the file's path relative to mount (not relative to start subvol),
		// e.g. "BackupComputer/BlackTower/foo/bar.txt".
		relMount := strings.TrimPrefix(file, mount+"/")
		lastFile = strings.TrimPrefix(file, live+"/")

		// Determine which (live) subvol owns this file → use that subvol's snapshots
		owner, subRel := liveMap.findForRel(relMount)
		if owner == nil || len(owner.snapshots) == 0 {
			cnt.notFound++
			return
		}
		snaps := owner.snapshots

		// Step 1: Find in oldest snapshot of the owning subvol
		snapIdx := findOldestSnap(snaps, subRel)
		if snapIdx < 0 {
			cnt.notFound++
			return
		}

		snap := snaps[snapIdx].absPath + "/" + subRel

		// Step 2: Size check
		sizeLive, _ := getFileSize(file)
		sizeSnap, _ := getFileSize(snap)
		if sizeLive != sizeSnap {
			cnt.changed++
			return
		}

		// Step 3: FIEMAP check
		t3 := time.Now()
		physLive, _, err1 := getFirstExtentPhys(file)
		debugTimed(t3, "fiemap live %s", file)
		t3 = time.Now()
		physSnap, _, err2 := getFirstExtentPhys(snap)
		debugTimed(t3, "fiemap snap %s", snap)
		if err1 == nil && err2 == nil && physLive == physSnap {
			cnt.shared++
			return
		}

		// === Candidate! ===
		cnt.candidates++

		// Step 4: Walk all snapshots OF THIS SUBVOL, collect extents
		t4 := time.Now()
		group := map[string]bool{file: true}
		uniqueExtentSizes := make(map[uint64]int64) // phys offset → physical extent size
		for _, s := range snaps {
			sf := s.absPath + "/" + subRel
			if !fileExists(sf) {
				continue
			}
			phys, physLen, err := getFirstExtentPhys(sf)
			if err != nil {
				group[sf] = true // can't check, include anyway
				continue
			}
			if phys == physLive {
				continue // already shared with live, skip
			}
			group[sf] = true
			if phys > 0 {
				uniqueExtentSizes[phys] = int64(physLen)
			}
		}
		debugTimed(t4, "walk %d snapshots for %s (%d unique extents)", len(snaps), lastFile, len(uniqueExtentSizes))

		// Step 5: For each unique extent, LOGICAL_INO to find all other refs
		t5 := time.Now()
		for phys := range uniqueExtentSizes {
			tLR := time.Now()
			resolved := logicalResolve(mountFd, mount, phys, subvolCache)
			debugTimed(tLR, "logicalResolve phys=0x%x → %d refs", phys, len(resolved))
			for _, p := range resolved {
				group[p] = true
			}
		}
		debugTimed(t5, "logicalResolve all %d extents for %s → %d total refs", len(uniqueExtentSizes), lastFile, len(group))

		cnt.copies += len(group)

		// Smart source selection:
		// 1. Compressed > uncompressed (preserve compression work)
		// 2. Fewer extents > more extents (preserve defrag work, threshold: 2x)
		// 3. More refs > fewer refs (less rewrite work, tiebreaker)
		//
		// Compare live extent vs the most common snapshot extent.
		bestSrc := file
		liveInfo, liveErr := getExtentInfo(file)

		if liveErr != nil || len(uniqueExtentSizes) == 0 {
			cnt.optDefault++
		} else if liveErr == nil && len(uniqueExtentSizes) > 0 {
			// Find a representative file for the largest snapshot extent group
			var snapRepresentative string
			for _, s := range snaps {
				sf := s.absPath + "/" + subRel
				if group[sf] && sf != file {
					snapRepresentative = sf
					break
				}
			}
			if snapRepresentative != "" {
				snapInfo, snapErr := getExtentInfo(snapRepresentative)
				if snapErr == nil {
					// Ref count: group total minus live-side refs
					// All paths sharing physLive are live-side, rest are snap-side
					snapRefs := len(group) - 1 // approximation: most group members are snap-side
					liveRefs := 1              // live file itself

					// Decision: prefer snapshot extent as source?
					preferSnap := false
					optReason := "default"

					if snapInfo.compressed && !liveInfo.compressed {
						preferSnap = true
						optReason = "compressed"
					} else if !snapInfo.compressed && liveInfo.compressed {
						preferSnap = false
						optReason = "compressed"
					} else {
						// Both same compression state — check fragmentation
						// Prefer less fragmented, but only if difference is significant (2x threshold)
						if snapInfo.numExtents > 0 && liveInfo.numExtents > 0 {
							if liveInfo.numExtents > snapInfo.numExtents*2 {
								preferSnap = true
								optReason = "lessFragmented"
							} else if snapInfo.numExtents > liveInfo.numExtents*2 {
								preferSnap = false
								optReason = "lessFragmented"
							} else {
								// Similar fragmentation — prefer more refs (less rewrite)
								if snapRefs > liveRefs {
									preferSnap = true
									optReason = "lessRewrites"
								}
							}
						} else if snapRefs > liveRefs {
							preferSnap = true
							optReason = "lessRewrites"
						}
					}

					switch optReason {
					case "compressed":
						cnt.optCompressed++
					case "lessFragmented":
						cnt.optLessFragmented++
					case "lessRewrites":
						cnt.optLessRewrites++
					default:
						cnt.optDefault++
					}

					if preferSnap {
						bestSrc = snapRepresentative
					}
				}
			}
		}

		// Build path list (bestSrc first)
		paths := make([]string, 0, len(group))
		paths = append(paths, bestSrc)
		for p := range group {
			if p != bestSrc {
				paths = append(paths, p)
			}
		}

		// Write to candidates.fdupes immediately
		writeFdupesGroup(paths)

		// Backpressure: wait if pending >= 10k (like CD burn buffer)
		for cnt.pending.Load() >= int64(QUEUE_LIMIT) {
			time.Sleep(100 * time.Millisecond)
		}

		// Send to dedup worker pool
		cnt.pending.Add(1)
		cnt.pendingCopies.Add(int64(len(paths)))
		// Sum physical sizes of unique extents for accurate saved calculation
		var estSaved int64
		for _, physLen := range uniqueExtentSizes {
			estSaved += physLen
		}
		cnt.pendingSaved.Add(estSaved)
		relFile := strings.TrimPrefix(file, live+"/")
		inflightAdd(relFile)
		dedupCh <- dedupGroup{paths: paths, estimatedSaved: estSaved, numCopies: len(paths), fileSize: sizeLive, relPath: relFile}
	}

	// Consumer: read from SpillQueue
	for !interrupted.Load() {
		file, ok := fileQ.Pop()
		if !ok {
			// Check if walker is done
			select {
			case <-walkDone:
				// findDone already set by walker goroutine
				// Drain remaining
				for !interrupted.Load() {
					f, ok := fileQ.Pop()
					if !ok {
						break
					}
					processFile(f)
				}
				goto done
			default:
				// Walker still running, queue temporarily empty — small sleep
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		processFile(file)
	}

done:
	close(dedupCh)
	if !interrupted.Load() {
		dedupWg.Wait() // wait for workers to finish
	}
	statusDone <- true
	fileQ.Cleanup()

	elapsed := int(time.Since(startTime).Seconds())
	saved := cnt.bytesSaved.Load()
	savedStr := fmt.Sprintf("%d B", saved)
	if saved >= 1024*1024*1024 {
		savedStr = fmt.Sprintf("%.2f GiB", float64(saved)/(1024*1024*1024))
	} else if saved >= 1024*1024 {
		savedStr = fmt.Sprintf("%.1f MiB", float64(saved)/(1024*1024))
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")
	fmt.Fprintf(os.Stderr, "Done: %d files checked (%d skipped)\n", cnt.checked, cnt.checked-cnt.candidates)
	fmt.Fprintf(os.Stderr, "  Candidates:     %d (%d total copies)\n", cnt.candidates, cnt.copies)
	fmt.Fprintf(os.Stderr, "  Skip not_found: %d\n", cnt.notFound)
	fmt.Fprintf(os.Stderr, "  Skip changed:   %d\n", cnt.changed)
	fmt.Fprintf(os.Stderr, "  Skip shared:    %d\n", cnt.shared)
	fmt.Fprintf(os.Stderr, "  Deduped:        %d (%d copies)\n", cnt.deduped.Load(), cnt.dedupedCopies.Load())
	fmt.Fprintf(os.Stderr, "  Pending:        %d (%d copies)\n", cnt.pending.Load(), cnt.pendingCopies.Load())
	fmt.Fprintf(os.Stderr, "  Saved:          %s\n", savedStr)
	fmt.Fprintf(os.Stderr, "  Runtime:        %s\n", fmtTime(elapsed))
	if interrupted.Load() {
		resumePoint := inflightOldest()
		if resumePoint == "" {
			resumePoint = lastFile
		}
		if resumePoint != "" {
			escaped := strings.ReplaceAll(resumePoint, "'", "'\\''")
			fmt.Fprintf(os.Stderr, "\n  Resume with: -start-at='%s'\n", escaped)
			fmt.Fprintf(os.Stderr, "  Unprocessed candidates in candidates.fdupes, done in candidates.done\n")
		}
	}
}

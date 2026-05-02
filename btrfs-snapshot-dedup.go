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

const VERSION = "0.3.0"

const (
	QUEUE_LIMIT    = 10000
	DEDUP_WORKERS  = 4

	SEARCH_KEY_SIZE    = C.sizeof_struct_btrfs_ioctl_search_key
	SEARCH_HEADER_SIZE = 32 // btrfs_ioctl_search_header is not in uapi, always 32

	BTRFS_ROOT_ITEM_KEY = 132

	FIEMAP_HEADER_SIZE = C.sizeof_struct_fiemap
	FIEMAP_EXTENT_SIZE = C.sizeof_struct_fiemap_extent
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

// Get physical offset of first extent via FIEMAP ioctl
func getFirstExtentPhys(path string) (uint64, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)

	buf := make([]byte, FIEMAP_HEADER_SIZE+FIEMAP_EXTENT_SIZE)
	binary.LittleEndian.PutUint64(buf[0:8], 0)           // fm_start
	binary.LittleEndian.PutUint64(buf[8:16], ^uint64(0))  // fm_length
	binary.LittleEndian.PutUint32(buf[24:28], 1)          // fm_extent_count

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		IOC_FIEMAP, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return 0, errno
	}

	mapped := binary.LittleEndian.Uint32(buf[20:24])
	if mapped == 0 {
		return 0, fmt.Errorf("no extents")
	}

	phys := binary.LittleEndian.Uint64(buf[FIEMAP_HEADER_SIZE+8 : FIEMAP_HEADER_SIZE+16])
	return phys, nil
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
	paths            []string // first path is src, rest are dests
	numUniqueExtents int      // for correct saved calculation
}

// fideduperange calls the FIDEDUPERANGE ioctl: dedup src into multiple dests.
// numUniqueExtents is used for saved calculation (unique old extents freed × file size).
// Returns (number of successfully deduped dests, total bytes saved).
func fideduperange(srcPath string, dstPaths []string, numUniqueExtents int) (int, int64, error) {
	srcFd, err := syscall.Open(srcPath, syscall.O_RDONLY, 0)
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

	// Actual space saved = unique old extents freed × file size
	savedBytes := int64(numUniqueExtents) * srcSize

	deduped := 0
	// Process in chunks of 120 dests (4KB page limit)
	for i := 0; i < len(dstPaths); i += 120 {
		end := i + 120
		if end > len(dstPaths) {
			end = len(dstPaths)
		}
		batch := dstPaths[i:end]

		bufSize := DEDUPE_RANGE_SIZE + len(batch)*DEDUPE_RANGE_INFO_SIZE
		buf := make([]byte, bufSize)
		le := binary.LittleEndian

		// file_dedupe_range header
		le.PutUint64(buf[0:8], 0)                        // src_offset
		le.PutUint64(buf[8:16], uint64(srcSize))          // src_length
		le.PutUint16(buf[16:18], uint16(len(batch)))      // dest_count

		// Open dest fds (O_RDONLY — works on read-only snapshots)
		dstFds := make([]int, len(batch))
		for j, dstPath := range batch {
			fd, err := syscall.Open(dstPath, syscall.O_RDONLY, 0)
			if err != nil {
				dstFds[j] = -1
				continue
			}
			dstFds[j] = fd
			off := DEDUPE_RANGE_SIZE + j*DEDUPE_RANGE_INFO_SIZE
			le.PutUint64(buf[off:off+8], uint64(fd)) // dest_fd
			le.PutUint64(buf[off+8:off+16], 0)       // dest_offset
			le.PutUint64(buf[off+16:off+24], 0)       // bytes_deduped (out)
			le.PutUint32(buf[off+24:off+28], 0)       // status (out)
		}

		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(srcFd),
			IOC_FIDEDUPERANGE, uintptr(unsafe.Pointer(&buf[0])))

		// Count successes and close fds
		for j := range batch {
			if dstFds[j] >= 0 {
				off := DEDUPE_RANGE_SIZE + j*DEDUPE_RANGE_INFO_SIZE
				status := int32(le.Uint32(buf[off+24 : off+28]))
				bytesDeduped := int64(le.Uint64(buf[off+16 : off+24]))
				if errno == 0 && status == 0 && bytesDeduped > 0 {
					deduped++
				}
				syscall.Close(dstFds[j])
			}
		}
		if errno != 0 {
			return deduped, savedBytes, errno
		}
	}
	return deduped, savedBytes, nil
}

type snapshotInfo struct {
	subvolID     uint64
	relPath      string // path relative to mount
	creationTime int64  // epoch seconds
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
	checked    int
	candidates int
	copies     int
	notFound   int
	changed    int
	shared     int
	deduped    atomic.Int64
	pending    atomic.Int64
	bytesSaved atomic.Int64
}

func main() {
	workers := flag.Int("workers", DEDUP_WORKERS, "number of parallel dedup workers")
	startAt := flag.String("start-at", "", "resume: skip files until this relative path (lexicographic)")
	flag.Parse()

	// First two positional args are mount + subvol, rest goes to find(1)
	args := flag.Args()
	if len(args) < 2 {
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
		fmt.Fprintf(os.Stderr, "  sudo %s -start-at 'path/to/last/file' /mnt/btrfs mysubvol\n", os.Args[0])
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

	// Find all snapshots via root tree scan + GET_SUBVOL_INFO (stable kernel API, no text parsing)
	fmt.Fprintf(os.Stderr, "Scanning for snapshots of %s...", subvol)
	snapInfos, err := getSubvolSnapshots(mountFd, mount, subvolUUID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nERROR: %v\n", err)
		os.Exit(1)
	}
	snapCount := len(snapInfos)
	if snapCount == 0 {
		fmt.Fprintf(os.Stderr, "\nERROR: no snapshots found\n")
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, " %d found.\n", snapCount)

	// Verify snapshots are accessible under the mount point
	testSnap := mount + "/" + snapInfos[0].relPath
	if _, err := os.Stat(testSnap); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: snapshot not accessible: %s\n", testSnap)
		fmt.Fprintf(os.Stderr, "Snapshots are resolved relative to the btrfs top-level.\n")
		fmt.Fprintf(os.Stderr, "If you use subvolume mounts (e.g. subvol=@), mount with subvolid=5 instead:\n")
		fmt.Fprintf(os.Stderr, "  mount -o subvolid=5 /dev/... /mnt/toplevel\n")
		fmt.Fprintf(os.Stderr, "  %s /mnt/toplevel %s\n", os.Args[0], subvol)
		os.Exit(1)
	}

	// Build snapshot absolute paths + subvol path cache
	type snapEntry struct {
		absPath      string
		creationTime int64
	}
	allSnaps := make([]snapEntry, snapCount)
	subvolCache := make(map[uint64]string) // root ID → subvol path (for logicalResolve)
	for i, si := range snapInfos {
		allSnaps[i] = snapEntry{
			absPath:      mount + "/" + si.relPath,
			creationTime: si.creationTime,
		}
		subvolCache[si.subvolID] = si.relPath
	}

	fmt.Fprintf(os.Stderr, "btrfs-snapshot-dedup v%s\n", VERSION)
	fmt.Fprintf(os.Stderr, "Mount:    %s\n", mount)
	fmt.Fprintf(os.Stderr, "Subvol:   %s (id=%d)\n", subvol, subvolID)
	fmt.Fprintf(os.Stderr, "Snapshots: %d (%s .. %s)\n", snapCount,
		snapInfos[0].relPath, snapInfos[snapCount-1].relPath)
	if len(findFilter) > 0 {
		fmt.Fprintf(os.Stderr, "Filter:   %s\n", strings.Join(findFilter, " "))
	}
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")
	fmt.Fprintln(os.Stderr, "  found      = files matching size filter (walker progress)")
	fmt.Fprintln(os.Stderr, "  buf        = files in queue waiting to be processed")
	fmt.Fprintln(os.Stderr, "  checked    = files processed / total (with ETA once walker completes)")
	fmt.Fprintln(os.Stderr, "  shared     = already shares extents with snapshot (skipped)")
	fmt.Fprintln(os.Stderr, "  not_found  = file not found in any snapshot (skipped)")
	fmt.Fprintln(os.Stderr, "  changed    = file size differs from snapshot (skipped)")
	fmt.Fprintln(os.Stderr, "  candidates = possible duplicates / total identical copies for dedup")
	fmt.Fprintln(os.Stderr, "  pending    = groups waiting for dedup worker")
	fmt.Fprintln(os.Stderr, "  deduped    = groups successfully deduplicated via FIDEDUPERANGE")
	fmt.Fprintln(os.Stderr, "  saved      = bytes saved by dedup (bytes_deduped from ioctl)")
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")

	// Binary search: find oldest snapshot containing a file
	findOldestSnap := func(rel string) int {
		hi := snapCount - 1
		if !fileExists(allSnaps[hi].absPath + "/" + rel) {
			return -1
		}
		if fileExists(allSnaps[0].absPath + "/" + rel) {
			return 0
		}
		lo := 0
		result := -1
		for lo <= hi {
			mid := (lo + hi) / 2
			if fileExists(allSnaps[mid].absPath + "/" + rel) {
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
			buf := fileQ.Buffered()
			saved := cnt.bytesSaved.Load()
			savedStr := fmt.Sprintf("%dB", saved)
			if saved >= 1024*1024*1024 {
				savedStr = fmt.Sprintf("%.1fG", float64(saved)/(1024*1024*1024))
			} else if saved >= 1024*1024 {
				savedStr = fmt.Sprintf("%.1fM", float64(saved)/(1024*1024))
			} else if saved >= 1024 {
				savedStr = fmt.Sprintf("%.1fK", float64(saved)/1024)
			}
			fmt.Fprintf(os.Stderr, "  [%s] found=%d buf=%d checked=%s shared=%d not_found=%d changed=%d candidates=%d/%d pending=%d deduped=%d saved=%s%s\n",
				fmtTime(elapsed), totalFound, buf, checkedStr, cnt.shared,
				cnt.notFound, cnt.changed, cnt.candidates, cnt.copies,
				cnt.pending.Load(), cnt.deduped.Load(), savedStr, etaStr)

			select {
			case <-statusDone:
				return
			case <-ticker.C:
			}
		}
	}()

	// Output files (append-only, fdupes format)
	fdupesFile, _ := os.OpenFile("candidates.fdupes", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer fdupesFile.Close()
	doneFile, _ := os.OpenFile("candidates.done", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer doneFile.Close()
	logFile, _ := os.OpenFile("bees-snapshot-dedup.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer logFile.Close()

	var fdupesMu sync.Mutex // protects fdupesFile writes
	var doneMu sync.Mutex   // protects doneFile writes

	writeFdupesGroup := func(paths []string) {
		fdupesMu.Lock()
		for _, p := range paths {
			fmt.Fprintln(fdupesFile, p)
		}
		fmt.Fprintln(fdupesFile)
		fdupesFile.Sync()
		fdupesMu.Unlock()
	}

	writeDoneGroup := func(paths []string) {
		doneMu.Lock()
		for _, p := range paths {
			fmt.Fprintln(doneFile, p)
		}
		fmt.Fprintln(doneFile)
		doneFile.Sync()
		doneMu.Unlock()
	}

	// Dedup worker pool
	dedupCh := make(chan dedupGroup, 100)
	var dedupWg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		dedupWg.Add(1)
		go func() {
			defer dedupWg.Done()
			for group := range dedupCh {
				src := group.paths[0]
				dsts := group.paths[1:]

				n, savedBytes, err := fideduperange(src, dsts, group.numUniqueExtents)
				if err != nil && n == 0 {
					fmt.Fprintf(logFile, "dedup error: %s: %v\n", src, err)
				}
				if n > 0 {
					cnt.bytesSaved.Add(savedBytes)
					writeDoneGroup(group.paths)
				}
				cnt.deduped.Add(1)
				cnt.pending.Add(-1)
			}
		}()
	}

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
		rel := strings.TrimPrefix(file, live+"/")
		lastFile = rel

		// Step 1: Find in oldest snapshot
		snapIdx := findOldestSnap(rel)

		if snapIdx < 0 {
			cnt.notFound++
			return
		}

		snap := allSnaps[snapIdx].absPath + "/" + rel

		// Step 2: Size check
		sizeLive, _ := getFileSize(file)
		sizeSnap, _ := getFileSize(snap)
		if sizeLive != sizeSnap {
			cnt.changed++
			return
		}

		// Step 3: FIEMAP check
		physLive, err1 := getFirstExtentPhys(file)
		physSnap, err2 := getFirstExtentPhys(snap)
		if err1 == nil && err2 == nil && physLive == physSnap {
			cnt.shared++
			return
		}

		// === Candidate! ===
		cnt.candidates++

		// Step 4: Walk all snapshots, collect extents, group by unique extent
		group := map[string]bool{file: true}
		uniqueExtents := make(map[uint64]bool) // physical offsets != live extent
		for _, s := range allSnaps {
			sf := s.absPath + "/" + rel
			if !fileExists(sf) {
				continue
			}
			phys, err := getFirstExtentPhys(sf)
			if err != nil {
				group[sf] = true // can't check, include anyway
				continue
			}
			if phys == physLive {
				continue // already shared with live, skip
			}
			group[sf] = true
			if phys > 0 {
				uniqueExtents[phys] = true
			}
		}

		// Step 5: For each unique extent, LOGICAL_INO to find all other refs
		for phys := range uniqueExtents {
			for _, p := range logicalResolve(mountFd, mount, phys, subvolCache) {
				group[p] = true
			}
		}

		cnt.copies += len(group)

		// Build path list (src = live file first)
		paths := make([]string, 0, len(group))
		paths = append(paths, file)
		for p := range group {
			if p != file {
				paths = append(paths, p)
			}
		}

		// Write to candidates.fdupes immediately
		writeFdupesGroup(paths)

		// Send to dedup worker pool
		cnt.pending.Add(1)
		dedupCh <- dedupGroup{paths: paths, numUniqueExtents: len(uniqueExtents)}
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
	fmt.Fprintf(os.Stderr, "  Deduped:        %d\n", cnt.deduped.Load())
	fmt.Fprintf(os.Stderr, "  Pending:        %d\n", cnt.pending.Load())
	fmt.Fprintf(os.Stderr, "  Saved:          %s\n", savedStr)
	fmt.Fprintf(os.Stderr, "  Runtime:        %s\n", fmtTime(elapsed))
	if interrupted.Load() && lastFile != "" {
		fmt.Fprintf(os.Stderr, "\n  Resume with: --start-at='%s'\n", lastFile)
		fmt.Fprintf(os.Stderr, "  Unprocessed candidates in candidates.fdupes, done in candidates.done\n")
	}
}

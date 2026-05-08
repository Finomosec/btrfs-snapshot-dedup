# btrfs-snapshot-dedup

There are cases where btrfs snapshots lose their shared extents with the original file. This causes the data to be stored twice — once in the live file, once in each snapshot — wasting significant disk space.

This tool re-consolidates snapshot extents with the live file. Fast, metadata-only candidate detection — no hashing. Just stat + FIEMAP + single-dest FIDEDUPERANGE with brute-force prefetch.

### Known causes of broken extent sharing

- **Defragmentation** — `btrfs filesystem defragment` rewrites extents, snapshots keep the old ones
- **Post-compression** — `btrfs filesystem defragment -czstd` creates new compressed extents, snapshots still reference the old uncompressed data
- **old bees dedup daemon** — Deduplicated live files but did not consolidate snapshot copies. This is fixed since version v0.11.

## How It Works

For each file in the live subvolume:

1. **Binary search** across snapshots to find the oldest copy (stat only)
2. **Size check** — if size differs, skip
3. **FIEMAP check** — compare physical extent offsets between live and snapshot
   - Same offset → already shared → skip
   - Different offset → candidate
4. **Collect group** — all snapshot copies + logical-resolve for other references
5. **Smart source selection** — compressed > less fragmented > fewer rewrites
6. **Single-dest FIDEDUPERANGE** — one ioctl per dest with brute-force pread prefetch

### Dedup Architecture (v1.3)

- **Single-dest dedup** — one FIDEDUPERANGE per dest, not multi-dest batching. Avoids kernel page-by-page thrashing across many dests.
- **pread prefetch** — brute-force `pread()` into a dummy buffer (128KB chunks) loads pages into the page cache before the ioctl. btrfs ignores `fadvise`/`readahead` (low ioprio), so direct reads are necessary (same approach as bees).
- **Background source refresh** — a goroutine re-reads the source file every 5s to prevent cache eviction during long dedup runs with many dests.
- **Phys-addr blacklist** — dests sorted by physical address. If the first dest with a given phys addr fails (content differs), all subsequent dests with the same phys addr are skipped.
- **4 size categories** — workers categorized by file size (<10MB unlimited, 10-100MB/100MB-1GB/>1GB max 1 each). Prevents I/O contention on HDDs.
- **Backpressure** — scanner pauses when pending groups reach 10,000. Prevents unbounded memory growth.

The kernel verifies each deduplication byte by byte — metadata comparison is only used to find candidates.

## Requirements

- Linux with btrfs filesystem
- Go 1.22+ with cgo (uses kernel headers for ioctl definitions)
- Root access (FIEMAP and FIDEDUPERANGE require it)

## Build

```bash
go build -o btrfs-snapshot-dedup btrfs-snapshot-dedup.go
```

## Usage

```bash
sudo ./btrfs-snapshot-dedup [options] <mountpoint> <subvolume> [find-filter...]
```

Everything after `<mountpoint> <subvolume>` is passed directly to `find(1)` as filter arguments. Default (no filter): `find <path> -type f` (all files).

Examples:

```bash
# Dedup all files
sudo ./btrfs-snapshot-dedup /mnt/btrfs mysubvol

# Only large files (>= 100MB)
sudo ./btrfs-snapshot-dedup /mnt/btrfs mysubvol -size +100M

# Only text/config files
sudo ./btrfs-snapshot-dedup /mnt/btrfs mysubvol '(' -iname '*.txt' -o -iname '*.json' -o -iname '*.xml' ')'

# Resume after interrupt
sudo ./btrfs-snapshot-dedup -start-at 'path/to/last/file' /mnt/btrfs mysubvol

# With debug logging (log actions >= 100ms)
sudo ./btrfs-snapshot-dedup -debug 100 /mnt/btrfs mysubvol

# Write candidate files for manual inspection
sudo ./btrfs-snapshot-dedup -write-candidates /mnt/btrfs mysubvol
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-workers` | `4` | Number of parallel dedup workers |
| `-start-at` | (none) | Resume: skip files before this path (lexicographic) |
| `-debug` | `0` | Debug log: log actions taking >= N ms (writes to `debug.log`) |
| `-write-candidates` | `false` | Write `candidates.fdupes` and `candidates.done` files |

## Output

### Live status (stderr, every 10 seconds)

```
[5m00s] found=767720 checked=10038/7920/0/0 pending=2084/58057/29.4G(371.7M,89.2M,9.7M,7.0M) deduped=33/918/83.8M skip=0 opt=524/242/1351/0 1% ETA:3h46m
```

| Field | Meaning |
|-------|---------|
| `found` | Files matching filter (walker progress) |
| `checked` | processed/shared/not_found/changed |
| `pending` | groups/copies/expectedSavings(active worker sizes) |
| `deduped` | groups/copies/saved |
| `skip` | Groups skipped by phys-addr blacklist |
| `opt` | Source selection: compressed/lessFragmented/lessRewrites/default |

### Output files

- **`debug.log`** — detailed timing log (only with `-debug`)
- **`candidates.fdupes`** — candidate groups in fdupes format (only with `-write-candidates`)
- **`candidates.done`** — successfully deduplicated groups (only with `-write-candidates`)

On interrupt (Ctrl+C), the last processed file path is shown for use with `-start-at`.

## Background: Why Snapshots Don't Follow

### Use Case 1: Dedup daemon catchup

When bees deduplicates file A and file B to share the same extent, it does not consolidate their snapshot copies. With 1000 snapshots, this means 1000 separate extents that are byte-identical but not shared. bees will eventually find them through its sequential extent-tree scan, but on a 22 TB filesystem this takes months.

### Use Case 2: Post-compression reclaim

After running `btrfs filesystem defragment -czstd /path`, each file gets new compressed extents (e.g. 3 GiB → 1.1 GiB). But every snapshot still references the old 3 GiB uncompressed extents. Net result: 1.1 + 3.0 = 4.1 GiB — worse than before compression.

Running `btrfs-snapshot-dedup` after compression points all snapshots to the compressed extents, freeing the old uncompressed ones.

## Technical Notes

### Why pread instead of fadvise/readahead?

`readahead()` is limited by `read_ahead_kb` (typically 128KB). `posix_fadvise(FADV_WILLNEED)` has no size limit but btrfs may ignore it or process it with low I/O priority. bees discovered this and uses brute-force `pread()` into a dummy buffer — the only reliable way to force pages into the page cache on btrfs.

### FIDEDUPERANGE on read-only snapshots

`FIDEDUPERANGE` works on read-only snapshots when dest files are opened with `O_RDONLY`. Opening with `O_RDWR` fails with `EROFS`.

### Accurate saved calculation

Multiple snapshot copies often share the same physical extent (COW), so deduplicating N snapshots from extent B to extent A only frees one copy of B, not N copies. This tool counts unique physical offsets among dests to report accurate savings.

## Related

- [bees](https://github.com/Zygo/bees) — btrfs deduplication daemon
- [duperemove](https://github.com/markfasheh/duperemove) — Hash-based btrfs deduplication tool

## License

GPL-2.0 (same as bees)

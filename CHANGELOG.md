# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [1.5.2]

### Fixed
- Filter arguments passed after `<mount> <subvol>` are now forwarded to
  `find(1)` verbatim as separate argv elements. They were previously split on
  whitespace, which broke quoted patterns containing spaces (for example a
  `-path` glob with spaces), so the filter matched no files.

### Changed
- The `find` command echoed at startup is now shell-quoted, making it
  unambiguous and safe to copy-paste (arguments containing spaces or glob
  characters are single-quoted).

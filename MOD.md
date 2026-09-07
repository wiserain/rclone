# Mod Branch Additions

This document records the functionality added or substantially changed by the
`mod` branch compared with upstream rclone. It is intended as a maintenance
map for future upstream merges, not as a replacement for the user manual.

The inventory includes the main integration points marked with `// mod` and the
new files that implement those features.

## Persistent VFS directory cache

The branch adds an opt-in, restart-safe VFS directory cache controlled by
`--dir-cache-persist`. When enabled, directory listings are stored below
`--cache-dir` and can be restored after the process restarts. The feature is
capability-based: a backend must implement the persistent directory cache
interfaces before the VFS uses it. It is currently available for 115 and
Google Drive remotes.

### VFS integration

The VFS integration is implemented in:

- [`vfs/persistent_dircache.go`](vfs/persistent_dircache.go)
- [`vfs/dir.go`](vfs/dir.go)
- [`vfs/vfs.go`](vfs/vfs.go)
- [`vfs/vfscommon/options.go`](vfs/vfscommon/options.go)
- [`fs/persistent_dircache.go`](fs/persistent_dircache.go)

The VFS restores a persisted listing on an in-memory cache miss, saves listings
after a successful remote read, and invalidates the corresponding persistent
directory or subtree after mutations. Explicit refreshes continue to obtain
fresh data from the remote. Pending virtual entries are never written as a
remote-confirmed persistent listing.

The cache lifecycle is tied to the VFS lifecycle. It is opened during VFS
creation, exposed in VFS RC statistics as `persistentDirCache`, closed during
shutdown, and purged by `VFS.CleanUp`.

Recursive refreshes use a mutation journal. A refresh can be committed as an
atomic whole-database or subtree replacement while concurrent invalidations
and saves are replayed afterward. This prevents a long refresh from restoring
stale directory state over a newer mutation.

### Storage and compatibility

[`vfs/vfsdircache`](vfs/vfsdircache) provides the storage layer:

- BoltDB-backed directory records keyed by canonical remote paths.
- Backend-private entry payloads supplied by a codec, allowing concrete files
  and directories to be reconstructed without an immediate metadata request.
- Adaptive raw or Zstandard-compressed records with size and corruption
  checks.
- Database identity derived from the remote configuration, root, filters,
  visible VFS options, backend identity, and codec version.
- Automatic preservation of an incompatible database as `.incompatible`
  before opening a new cache.
- Atomic snapshot installation with recovery from interrupted swaps.
- Cache hit, miss, expiry, write, mutation, and error statistics.

The persistent database schema is currently version 3. The schema bump is
intentional: older cache databases are invalidated when path-normalization or
path-safety assumptions change. Future changes that alter the meaning of
stored paths or records must either preserve compatibility explicitly or bump
the schema/codec version.

## Direct access by folder or file ID

This is a shared extension across the 115, PikPak, and Google Drive backends.
It lets a remote select a folder or file by its service ID using
`remote:{ID}`, with an optional `/subpath` below a folder root. URL forms must
also be enclosed in braces. This is a common user-facing convention implemented
in each backend's `mod.go`, constructor, and object lookup; it is not a generic
path parser available to every backend and does not require persistent caching.

| Backend | Accepted values inside braces | Implementation |
| --- | --- | --- |
| 115 | 19-digit IDs, CID URLs, supported share links | [mod.go](backend/115/mod.go), [115.go](backend/115/115.go) |
| PikPak | IDs and drive URLs under `/drive/all/` or `/drive/recent/` | [mod.go](backend/pikpak/mod.go), [pikpak.go](backend/pikpak/pikpak.go) |
| Google Drive | IDs, folder/file URLs, and URLs containing an `id` parameter | [mod.go](backend/drive/mod.go), [drive.go](backend/drive/drive.go) |

Folder IDs override the configured root for that remote instance. File IDs
produce a file-root remote using `fs.ErrorIsFile` and a retained object for
later lookup. They are intended for single-object operations such as copy;
directory-listing behavior must not be assumed for file roots. In particular,
the 115 implementation documents listing and `cat` limitations for this form.
115 share links select a share context with its receive code instead of a
normal account folder.

Each backend also provides `rclone backend getid remote:path [subpath]` to
resolve a path to an ID. Drive's `-o real` returns the shortcut target ID.

The copy command recognizes the file-root form returned by these backends and
copies it as one file. This is what enables commands such as
`rclone copy remote:{FILE_ID} destination:`. The command-level integration is
in [`cmd/copy/copy.go`](cmd/copy/copy.go).

Examples (replace the IDs and remote names with configured values):

```console
rclone lsf 'drive:{FOLDER_ID}/subdir'
rclone copy 'pikpak:{FILE_ID}' ./downloads
rclone backend getid drive:folder file.txt -o real
```

## Dedupe selection modes

[`fs/operations/dedupe.go`](fs/operations/dedupe.go) adds two modes to the
shared dedupe operation:

- `longest`: keep the object with the longest `Object.String()` value.
- `shortest`: keep the object with the shortest `Object.String()` value.

The comparison uses Go's `len(string)`: byte length, not Unicode character
count, file size, or media duration. For typical backends the string is the
object's remote path, so parent directory names contribute to its length.
Equal lengths have no guaranteed tie-breaking order.

These modes are particularly useful with `--by-hash`, where files with the
same content can have different paths. Name-based dedupe retains upstream's
directory merging and identical-file removal before applying the selected
mode; duplicates with the same path usually have equal string lengths.

```console
rclone dedupe --by-hash longest remote:path --dry-run
rclone dedupe --by-hash --dedupe-mode shortest remote:path --dry-run
```

Both positional mode names and `--dedupe-mode` accept these values, although
the existing command help does not list them. Deletion uses the existing
dedupe deletion path and honors `--dry-run`.

## 115 backend

The branch adds the 115 backend and registers it through
[`backend/all/all.go`](backend/all/all.go). The implementation and its tests
are under [`backend/115`](backend/115), with backend metadata in
[`docs/data/backends/115.yaml`](docs/data/backends/115.yaml).

### Remote and object support

The backend provides cookie-based authentication, directory and object
operations, shares, downloads, SHA-1 based upload handling, single-part and
OSS multipart uploads, and the associated 115 API error and response types.
It includes options for upload cutoff/chunking, upload concurrency, hash-only
uploads, upload history, internal and dual-stack upload endpoints, upload
verification, download cookies, and CDN proxy control.

Direct ID roots and `getid` are covered in
[Direct access by folder or file ID](#direct-access-by-folder-or-file-id).

The backend-specific `getid` command returns the ID for a directory or object.
Other backend commands include adding offline download URLs, importing a
share, and retrieving file statistics. Share remotes retain separate share
identity and receive-code handling, and operations unsupported by a shared
filesystem are rejected.

### Upload integrity and protocol compatibility

The multipart uploader checks the number of bytes read before finalizing an
upload. The single-part uploader performs the equivalent check with a counting
reader. If a source ends before its declared size, the upload returns
`io.ErrUnexpectedEOF` instead of leaving a truncated object reported as
successful. Sequential multipart uploads also recover from a
`PartAlreadyExist` conflict by checking the already uploaded part's size and
ETag before continuing; this handles an upload that may have succeeded even
though its response was lost.

The multipart upload recovery is implemented in
[`backend/115/multipart.go`](backend/115/multipart.go).

The 115 upload protocol uses custom encryption and legacy RSA
PKCS#1 v1.5 encryption for wire compatibility. The narrow staticcheck
suppression around that operation is deliberate; replacing it with OAEP would
change the 115 protocol and is not a source-compatible security cleanup.

### Persistent directory cache codec

[`backend/115/persistent_dircache.go`](backend/115/persistent_dircache.go)
implements the VFS persistent-cache codec. It stores directory IDs and object
metadata including file IDs, parent IDs, sizes, SHA-1 values, pickcodes, and
modification times. Restoring a directory also repopulates the 115 path-to-ID
directory cache, avoiding a subsequent path traversal request.

The cache identity includes the account, selected root, and share state. A
cache must therefore not be reused across different accounts, roots, or share
links.

## PikPak extensions

The PikPak changes are concentrated in [`backend/pikpak/mod.go`](backend/pikpak/mod.go)
and the marked integration points in [`backend/pikpak/pikpak.go`](backend/pikpak/pikpak.go).

### Backend commands

Direct ID roots and shared `getid` behavior are covered in
[Direct access by folder or file ID](#direct-access-by-folder-or-file-id).

The added backend commands are:

- `checkurl`: queries resource metadata and cache status for one or more URLs;
  it can optionally add offline-download tasks for all, cached-only, or
  uncached resources.
- `getid`: returns the ID of a file or directory.
- `redeem`: submits an activation code and returns the server's JSON result.

The supporting resource and redeem response types live in `mod.go`; they are
kept separate from the upstream PikPak API types because they describe the
additional endpoints used by these commands.

## Google Drive extensions

Google Drive integration is spread across the marked sections of
[`backend/drive/drive.go`](backend/drive/drive.go),
[`backend/drive/upload.go`](backend/drive/upload.go), and
[`backend/drive/mod.go`](backend/drive/mod.go).

### Service-account pooling and rotation

The backend can build a pool from a directory of service-account JSON files,
combine those files with a JSON list of impersonated users, and load/shuffle
credential combinations up to a configured limit. On eligible retry or
rate-limit paths it can replace the active OAuth and Drive clients, reset the
pacer, and continue with another account. With
`service_account_per_file`, uploads/copies can request a service-account
change for each file.

The related options are:

- `service_account_file_path`
- `service_account_min_sleep`
- `service_account_per_file`
- `service_account_max_load`
- `impersonate_list`

### Custom GDS authentication

The `gds_userid`, `gds_apikey`, `gds_endpoint`, and `gds_mode` options enable a
custom authentication service. The service supplies service-account data,
scope, impersonation, and root-folder information. The resulting Drive client
is retained for GDS listing and query operations, while Drive Activity uses
the matching authenticated client when enabled.

### Drive Activity notifications

`activity_targets` enables polling of the Google Drive Activity API for the
configured folder IDs. `activity_sleep` spaces requests for multiple targets.
Create, edit, move, rename, delete, and restore activities are translated into
VFS change notifications and directory-cache updates where the affected paths
can be resolved. The feature supplements the normal Drive change-notification
path and is disabled when no activity targets are configured.

### Backend commands

Direct ID roots and shortcut ID resolution are covered in
[Direct access by folder or file ID](#direct-access-by-folder-or-file-id).

The added backend commands are:

- `getid`: returns a file or directory ID, optionally resolving a shortcut to
  its target.
- `getfile`: returns the selected file metadata as JSON, with an `all` option
  for development/debugging.
- `chpar`: changes parents to move an object tree between Drive remotes;
  depth 0/1 and removal of an empty source directory are supported.

### Persistent directory cache codec

[`backend/drive/persistent_dircache.go`](backend/drive/persistent_dircache.go)
serializes Drive directories, regular objects, exported documents, and link
objects. It preserves IDs, parent IDs, MIME types, modified dates, sizes,
resource keys, metadata, checksums, V2-download state, export information,
and link contents. Restoring a directory repopulates both the Drive path
cache and resource-key map.

The Drive cache identity includes the root/team-drive context, authorization
scope, listing and shortcut behavior, trash/shared/starred filters, export and
encoding settings, metadata mode, and other options that affect the visible
tree.

## CI, release, and installation support

The branch-specific workflow additions are in
[`.github/workflows/build.yml`](.github/workflows/build.yml):

- `jobs.build` omits the `other_os` and `go1.26` compatibility jobs, builds the
  regular binaries with `noselfupdate`, skips the race test for release tags,
  and uploads all build outputs for tags or selected test artifacts otherwise.
- `jobs.termux` calculates release and branch-safe beta version values, checks
  out `termux/termux-packages`, builds aarch64 and arm packages from this
  branch, and uploads release or test artifacts as appropriate.
- `jobs.release` waits for the build and Termux jobs, downloads their artifacts,
  creates checksums, creates a draft GitHub release for `wiserain/rclone`, and
  uploads the assets. Publishing the draft remains a manual step.
- The former automatic Winget publishing workflow is removed, and the legacy
  `make ci_beta` uploads to `beta.rclone.org` are disabled.

[`install.sh`](install.sh) is a mod-specific installer. It downloads from the
mod repository's releases, supports an optional tag argument, handles Linux,
BSD, macOS, and Termux package installation, and accepts `unzip`, `7z`, or
BusyBox as archive tools.

## Upstream merge and maintenance notes

- Before and after an upstream merge, search for `// mod` and review the
  shared code touched by the branch, especially VFS, dedupe, copy, Drive, and
  PikPak.
- The 115 backend is new and absent from upstream, so its files normally merge
  independently. Recheck it when upstream changes shared `fs` interfaces,
  upload APIs, toolchain requirements, or adds a backend at the same path.
- If upstream adds an equivalent feature, compare the implementations before
  keeping both. Persistent cache format changes require an appropriate codec
  or database schema update.

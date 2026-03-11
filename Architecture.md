# Jet Current Architecture

This document reflects the current codebase, not the original design target.

The diagrams below are split by concern so the file stays readable:

- local storage and ingest
- local workspace state
- remote sync path
- metadata-only history

## 1. Local Storage And Ingest

```mermaid
flowchart LR
    A["Workspace files"] --> B["jet add"]
    B --> C["collect_files()"]
    C --> D{"ManifestIndex fast path"}
    D -->|unchanged| E["reuse existing file entry"]
    D -->|changed| F{"file size"}
    F -->|small| G["direct blob path"]
    F -->|large| H["chunked path"]
    H --> I["FastCDC + chunk ids + file digest"]
    G --> J["FsObjectStore"]
    I --> J
    E --> K["StagingIndex"]
    J --> K
    K --> L["jet commit"]
    L --> M["CommitStore"]
    J --> N["Segment files + segment index"]
```

Notes:

- small files use the direct blob path
- large files use chunking and then batch object writes
- `jet commit` is light; most work happens during `jet add`

## 2. Local Workspace State

```mermaid
flowchart LR
    A["jet open"] --> B["workspace.bin"]
    A --> C["workspace-manifest.bin"]
    A --> D["materialized-index.bin"]
    A --> E["metadata-first switch"]

    E --> C
    E --> D

    F["jet hydrate"] --> C
    F --> D
    F --> G["resolve file set"]
    G --> H["restore files"]
    H --> I["Workspace files"]
    H --> D

    J["jet dehydrate"] --> D
    J --> K["remove clean materialized files"]

    L["jet status"] --> B
    L --> C
    L --> D
```

Notes:

- `jet open` does not fully materialize the repo
- the current visible workspace file set lives in `workspace-manifest.bin`
- `materialized-index.bin` tracks `virtual / hydrated / dirty / not-in-view`

## 3. Remote Sync Path

```mermaid
flowchart LR
    A["jet CLI"] --> B["jet-remote"]
    B --> C["gRPC transport"]
    C --> D["jet-server"]
    D --> E["Repo on disk"]

    D --> F["GetManifest (paged)"]
    D --> G["StreamChunks"]
    D --> H["GetCommit / GetHead"]
    D --> I["PutChunks / PutCommit / UpdateHead"]

    F --> B
    G --> B
    H --> B
    I --> D

    B --> J["workspace-manifest.bin"]
    B --> K["local object store"]
    B --> L["open / hydrate / pull / push"]
```

Notes:

- remote clone, pull, open, and hydrate are manifest-first
- remote object transfer is stream-based
- server-side manifest filtering happens before local hydrate/open decisions

## 4. Metadata-Only History

```mermaid
flowchart TD
    A["remote clone / pull"] --> B["sync commit metadata"]
    B --> C{"current workspace commit?"}
    C -->|yes| D["workspace manifest drives file view"]
    C -->|no| E["store metadata-only commit"]
    E --> F["files_omitted = true"]

    G["jet log / jet stats"] --> E
    H["jet open <old-commit>"] --> I{"metadata present locally?"}
    I -->|yes| J["open commit"]
    I -->|no| K["fetch missing commit metadata from remote"]
    K --> J
```

Notes:

- remote history does not need every commit to be fully materialized locally
- older commits can exist as lightweight metadata-only records
- opening an older remote commit can fetch missing metadata on demand

## 5. Crate Boundaries

```mermaid
flowchart LR
    A["jet-cli"] --> B["jet-core"]
    A --> C["jet-remote"]
    C --> B
    C --> D["jet-proto"]
    E["jet-server"] --> B
    E --> D
```

Notes:

- `jet-core` owns local repo logic
- `jet-remote` owns remote client workflows
- `jet-server` owns service-side RPC handlers
- `jet-proto` owns shared protocol definitions

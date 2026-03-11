use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use memmap2::MmapOptions;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::chunk_cache::ChunkCache;
use crate::chunking::{Chunker, FastCdcChunker};
use crate::commit_store::{Commit, CommitChunkRef, CommitFileEntry, CommitStore, FsCommitStore};
use crate::error::{JetError, Result};
use crate::manifest_index::{ManifestEntry, ManifestIndex, modified_key};
use crate::object_store::{CheckoutChunkSource, CheckoutSession, FsObjectStore};
use crate::repo::{RepoConfig, load_repo_config};
use crate::trace;
use crate::workspace::{
    MaterializedEntry, MaterializedIndex, MaterializedState, WorkspaceLocalConfig, WorkspaceState,
    load_materialized_index, load_workspace_local_config, load_workspace_manifest,
    load_workspace_state, save_materialized_index, save_workspace_local_config,
    save_workspace_manifest, save_workspace_state,
};

const LARGE_FILE_CHUNK_CACHE_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagingIndex {
    pub schema_version: u32,
    pub files: Vec<CommitFileEntry>,
}

impl Default for StagingIndex {
    fn default() -> Self {
        Self {
            schema_version: 1,
            files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RepoStats {
    pub object_count: u64,
    pub object_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckMode {
    Quick,
    Deep,
}

#[derive(Debug, Clone)]
pub struct WorkspaceStatus {
    pub current_commit_id: Option<String>,
    pub remote_source: Option<String>,
    pub virtual_count: usize,
    pub hydrated_count: usize,
    pub dirty_count: usize,
    pub pending_count: usize,
    pub not_in_view_count: usize,
    pub view_includes: Vec<String>,
    pub view_excludes: Vec<String>,
    pub dirty_paths: Vec<String>,
    pub pending_paths: Vec<String>,
    pub not_in_view_paths: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct JetRepository {
    root: PathBuf,
    config: RepoConfig,
    chunker: FastCdcChunker,
    object_store: FsObjectStore,
    commit_store: FsCommitStore,
}

#[derive(Debug, Clone)]
struct CandidateFile {
    absolute: PathBuf,
    relative_path: String,
    metadata_len: u64,
    modified_unix_secs: u64,
    modified_nanos: u32,
}

#[derive(Debug, Clone)]
struct CheckoutTask {
    path: String,
    file_digest: String,
    chunk_count: usize,
    size: u64,
    chunks: Vec<CheckoutChunkSource>,
}

impl JetRepository {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        maybe_init_rayon_pool();
        let root = path.as_ref().to_path_buf();
        let config = load_repo_config(&root)?;
        let chunker = FastCdcChunker {
            min_size: config.chunking.min,
            avg_size: config.chunking.avg,
            max_size: config.chunking.max,
        };
        let object_store =
            FsObjectStore::new(&root, config.compression.enabled, config.compression.level)?;
        let commit_store = FsCommitStore::new(&root)?;

        Ok(Self {
            root,
            config,
            chunker,
            object_store,
            commit_store,
        })
    }

    pub fn add_paths(&self, paths: &[PathBuf]) -> Result<usize> {
        let trace_add = trace::add_enabled();
        let add_started = Instant::now();
        let mut staging = self.load_staging()?;
        let after_staging_load = Instant::now();
        let mut manifest_index = ManifestIndex::load(&self.root)?;
        let after_manifest_load = Instant::now();
        let mut chunk_cache = ChunkCache::load(&self.root)?;
        let after_chunk_cache_load = Instant::now();
        let live_chunk_cache = Arc::new(Mutex::new(HashMap::<String, Vec<CommitChunkRef>>::new()));
        let files = self.collect_files(paths)?;
        let after_collect = Instant::now();
        let processed = files
            .par_iter()
            .map(|path| self.process_file(path, &manifest_index, &chunk_cache, &live_chunk_cache))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let after_process = Instant::now();
        let mut trace_totals = AddFileTrace::default();

        let mut staged_by_path: HashMap<String, CommitFileEntry> = staging
            .files
            .drain(..)
            .map(|file| (file.path.clone(), file))
            .collect();

        let mut reused_count = 0usize;
        let mut new_count = 0usize;
        for file in processed {
            trace_totals.small_file_count += file.trace.small_file_count;
            trace_totals.large_file_count += file.trace.large_file_count;
            trace_totals.reused_unchanged_count += file.trace.reused_unchanged_count;
            trace_totals.reused_same_content_count += file.trace.reused_same_content_count;
            trace_totals.cache_hit_count += file.trace.cache_hit_count;
            trace_totals.reused_boundary_count += file.trace.reused_boundary_count;
            trace_totals.small_read_ms += file.trace.small_read_ms;
            trace_totals.small_store_ms += file.trace.small_store_ms;
            trace_totals.large_digest_ms += file.trace.large_digest_ms;
            trace_totals.large_chunking_ms += file.trace.large_chunking_ms;
            trace_totals.large_store_ms += file.trace.large_store_ms;
            if !file.was_reused {
                new_count += 1;
                manifest_index.upsert(
                    file.entry.path.clone(),
                    file.metadata_len,
                    file.modified_unix_secs,
                    file.modified_nanos,
                    file.entry.clone(),
                );
            } else {
                reused_count += 1;
            }
            staged_by_path.insert(file.entry.path.clone(), file.entry);
        }
        for (file_digest, refs) in live_chunk_cache.lock().expect("chunk cache lock").drain() {
            chunk_cache.insert(file_digest, refs);
        }

        staging.files = staged_by_path.into_values().collect();
        self.save_staging(&staging)?;
        manifest_index.save(&self.root)?;
        chunk_cache.save(&self.root)?;
        let after_save = Instant::now();
        if trace_add {
            trace::emit_add(
                elapsed_ms(after_staging_load.duration_since(add_started)),
                elapsed_ms(after_manifest_load.duration_since(after_staging_load)),
                elapsed_ms(after_chunk_cache_load.duration_since(after_manifest_load)),
                elapsed_ms(after_collect.duration_since(after_chunk_cache_load)),
                elapsed_ms(after_process.duration_since(after_collect)),
                elapsed_ms(after_save.duration_since(after_process)),
                files.len(),
                reused_count,
                new_count,
                trace_totals.small_file_count,
                trace_totals.large_file_count,
                trace_totals.reused_unchanged_count,
                trace_totals.reused_same_content_count,
                trace_totals.cache_hit_count,
                trace_totals.reused_boundary_count,
                trace_totals.small_read_ms,
                trace_totals.small_store_ms,
                trace_totals.large_digest_ms,
                trace_totals.large_chunking_ms,
                trace_totals.large_store_ms,
                elapsed_ms(after_save.duration_since(add_started)),
            );
        }
        Ok(staging.files.len())
    }

    fn collect_files(&self, paths: &[PathBuf]) -> Result<Vec<CandidateFile>> {
        let mut files = Vec::new();
        for path in paths {
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                self.root.join(path)
            };

            if absolute.starts_with(self.root.join(".jet")) {
                continue;
            }

            let metadata = fs::metadata(&absolute)?;
            if metadata.is_dir() {
                let mut entries = Vec::new();
                collect_dir_file_entries(&absolute, &mut entries)?;
                for (entry, metadata) in entries {
                    if !entry.starts_with(self.root.join(".jet")) {
                        files.push(self.to_candidate(entry, metadata)?);
                    }
                }
            } else {
                files.push(self.to_candidate(absolute, metadata)?);
            }
        }

        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        files.dedup_by(|a, b| a.relative_path == b.relative_path);
        Ok(files)
    }

    fn process_file(
        &self,
        candidate: &CandidateFile,
        manifest_index: &ManifestIndex,
        chunk_cache: &ChunkCache,
        live_chunk_cache: &Arc<Mutex<HashMap<String, Vec<CommitChunkRef>>>>,
    ) -> Result<ProcessedFile> {
        let mut trace = AddFileTrace::default();
        if let Some(cached) = manifest_index.get_if_unchanged(
            &candidate.relative_path,
            candidate.metadata_len,
            candidate.modified_unix_secs,
            candidate.modified_nanos,
        ) {
            trace.reused_unchanged_count = 1;
            return Ok(ProcessedFile {
                entry: cached,
                metadata_len: candidate.metadata_len,
                modified_unix_secs: candidate.modified_unix_secs,
                modified_nanos: candidate.modified_nanos,
                was_reused: true,
                trace,
            });
        }

        let direct_blob_threshold = self.config.storage.direct_blob_threshold_bytes;
        let previous_entry = manifest_index.entries.get(&candidate.relative_path);
        let (refs, file_digest, file_size) = if candidate.metadata_len <= direct_blob_threshold {
            trace.small_file_count = 1;
            let read_started = Instant::now();
            let data = fs::read(&candidate.absolute)?;
            let after_read = Instant::now();
            let file_digest = blake3::hash(&data).to_hex().to_string();
            if let Some(previous) = previous_entry
                && previous.file.file_digest == file_digest
            {
                trace.reused_same_content_count = 1;
                trace.small_read_ms = elapsed_ms(after_read.duration_since(read_started));
                return Ok(ProcessedFile {
                    entry: previous.file.clone(),
                    metadata_len: candidate.metadata_len,
                    modified_unix_secs: candidate.modified_unix_secs,
                    modified_nanos: candidate.modified_nanos,
                    was_reused: false,
                    trace,
                });
            }
            if !self.object_store.has_chunk_id(&file_digest)? {
                self.object_store
                    .ensure_chunk_with_id(&file_digest, &data)?;
            }
            let after_store = Instant::now();
            trace.small_read_ms = elapsed_ms(after_read.duration_since(read_started));
            trace.small_store_ms = elapsed_ms(after_store.duration_since(after_read));
            (
                vec![CommitChunkRef {
                    id: file_digest.clone(),
                    offset: 0,
                    len: data.len() as u64,
                    raw_len: data.len() as u64,
                }],
                file_digest,
                data.len() as u64,
            )
        } else {
            trace.large_file_count = 1;
            let trace_large = trace::large_add_enabled();
            let large_started = Instant::now();
            let file_handle = fs::File::open(&candidate.absolute)?;
            let mmap = unsafe { MmapOptions::new().map(&file_handle)? };
            let data: &[u8] = &mmap;
            let use_large_cache = candidate.metadata_len >= LARGE_FILE_CHUNK_CACHE_THRESHOLD_BYTES;
            if let Some(previous) = previous_entry
                && previous.size == data.len() as u64
            {
                let digest_started = Instant::now();
                if self.large_file_matches_previous_chunks(previous, data) {
                    let after_digest = Instant::now();
                    trace.large_digest_ms = elapsed_ms(after_digest.duration_since(digest_started));
                    trace.reused_same_content_count = 1;
                    if trace_large {
                        trace::emit_large_add(
                            &candidate.relative_path,
                            candidate.metadata_len / (1024 * 1024),
                            previous.file.chunks.len(),
                            0u128,
                            0u128,
                            trace.large_digest_ms,
                            elapsed_ms(large_started.elapsed()),
                        );
                    }
                    return Ok(ProcessedFile {
                        entry: previous.file.clone(),
                        metadata_len: candidate.metadata_len,
                        modified_unix_secs: candidate.modified_unix_secs,
                        modified_nanos: candidate.modified_nanos,
                        was_reused: false,
                        trace,
                    });
                }
                let after_digest = Instant::now();
                trace.large_digest_ms = elapsed_ms(after_digest.duration_since(digest_started));
            }
            let should_prehash_large = use_large_cache
                || previous_entry.is_some_and(|previous| previous.size == data.len() as u64);
            let cache_key = if should_prehash_large {
                let digest_started = Instant::now();
                let mut hasher = blake3::Hasher::new();
                hasher.update_rayon(data);
                let digest = hasher.finalize().to_hex().to_string();
                let after_digest = Instant::now();
                trace.large_digest_ms += elapsed_ms(after_digest.duration_since(digest_started));
                Some(digest)
            } else {
                None
            };

            if let Some(file_digest) = cache_key.as_ref()
                && let Some(previous) = previous_entry
                && previous.file.file_digest == *file_digest
            {
                trace.reused_same_content_count = 1;
                if trace_large {
                    trace::emit_large_add(
                        &candidate.relative_path,
                        candidate.metadata_len / (1024 * 1024),
                        previous.file.chunks.len(),
                        0u128,
                        0u128,
                        trace.large_digest_ms,
                        elapsed_ms(large_started.elapsed()),
                    );
                }
                return Ok(ProcessedFile {
                    entry: previous.file.clone(),
                    metadata_len: candidate.metadata_len,
                    modified_unix_secs: candidate.modified_unix_secs,
                    modified_nanos: candidate.modified_nanos,
                    was_reused: false,
                    trace,
                });
            }

            if let Some(file_digest) = cache_key.as_ref()
                && let Some(cached_refs) = chunk_cache.get(file_digest)
            {
                trace.cache_hit_count = 1;
                if trace_large {
                    trace::emit_large_add(
                        &candidate.relative_path,
                        candidate.metadata_len / (1024 * 1024),
                        cached_refs.len(),
                        0u128,
                        0u128,
                        trace.large_digest_ms,
                        elapsed_ms(large_started.elapsed()),
                    );
                }
                (cached_refs.clone(), file_digest.clone(), data.len() as u64)
            } else if let Some(file_digest) = cache_key.as_ref()
                && let Some(cached_refs) = live_chunk_cache
                    .lock()
                    .expect("chunk cache lock")
                    .get(file_digest)
                    .cloned()
            {
                trace.cache_hit_count = 1;
                if trace_large {
                    trace::emit_large_add(
                        &candidate.relative_path,
                        candidate.metadata_len / (1024 * 1024),
                        cached_refs.len(),
                        0u128,
                        0u128,
                        trace.large_digest_ms,
                        elapsed_ms(large_started.elapsed()),
                    );
                }
                (cached_refs, file_digest.clone(), data.len() as u64)
            } else {
                let chunking_started = Instant::now();
                let chunks = if use_large_cache {
                    if let Some(reused) =
                        self.reuse_previous_large_file_chunks(candidate, manifest_index, data)
                    {
                        trace.reused_boundary_count = 1;
                        reused
                    } else {
                        self.chunker.chunk_bytes(data)
                    }
                } else {
                    self.chunker.chunk_bytes(data)
                };
                trace.large_chunking_ms = elapsed_ms(chunking_started.elapsed());
                let (prepared, file_digest, hash_ms) = self.prepare_large_chunk_refs(data, &chunks);
                if !should_prehash_large {
                    trace.large_digest_ms = hash_ms;
                }

                let out = if let Some(previous) = previous_entry
                    && previous.file.file_digest == file_digest
                {
                    trace.reused_same_content_count = 1;
                    (
                        previous.file.chunks.clone(),
                        previous.file.file_digest.clone(),
                        previous.file.size,
                    )
                } else {
                    let store_started = Instant::now();
                    let batch = prepared
                        .iter()
                        .map(|chunk| {
                            (
                                chunk.id.as_str(),
                                &data[chunk.offset..chunk.offset + chunk.length],
                            )
                        })
                        .collect::<Vec<_>>();
                    self.object_store.ensure_chunks_with_ids(&batch)?;
                    trace.large_store_ms = elapsed_ms(store_started.elapsed());
                    let refs = prepared
                        .iter()
                        .map(|chunk| CommitChunkRef {
                            id: chunk.id.clone(),
                            offset: chunk.offset as u64,
                            len: chunk.length as u64,
                            raw_len: chunk.length as u64,
                        })
                        .collect::<Vec<_>>();
                    if use_large_cache {
                        live_chunk_cache
                            .lock()
                            .expect("chunk cache lock")
                            .insert(file_digest.clone(), refs.clone());
                    }
                    (refs, file_digest, data.len() as u64)
                };

                if trace_large {
                    trace::emit_large_add(
                        &candidate.relative_path,
                        candidate.metadata_len / (1024 * 1024),
                        chunks.len(),
                        trace.large_chunking_ms,
                        trace.large_store_ms,
                        trace.large_digest_ms,
                        elapsed_ms(large_started.elapsed()),
                    );
                }
                out
            }
        };

        let entry = CommitFileEntry {
            path: candidate.relative_path.clone(),
            size: file_size,
            file_digest,
            chunks: refs,
        };
        Ok(ProcessedFile {
            entry,
            metadata_len: candidate.metadata_len,
            modified_unix_secs: candidate.modified_unix_secs,
            modified_nanos: candidate.modified_nanos,
            was_reused: false,
            trace,
        })
    }

    pub fn commit(&self, message: &str, author: &str) -> Result<String> {
        let mut staging = self.load_staging()?;
        if staging.files.is_empty() {
            return Err(JetError::EmptyStaging);
        }

        staging.files.sort_by(|a, b| a.path.cmp(&b.path));

        let parent = self.commit_store.read_head()?;
        let timestamp_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| JetError::InvalidSystemTime)?
            .as_secs() as i64;

        let id = commit_id(&parent, message, timestamp_unix, &staging.files);
        let commit = Commit {
            schema_version: self.config.schema_version,
            id: id.clone(),
            parent,
            author: author.to_string(),
            message: message.to_string(),
            timestamp_unix,
            files_omitted: false,
            files: staging.files,
        };

        self.commit_store.write_commit(&commit)?;
        self.commit_store.write_head(&commit.id)?;
        self.save_staging(&StagingIndex::default())?;
        self.sync_materialized_after_commit(&commit)?;
        let mut workspace = self.load_workspace_state()?;
        workspace.current_commit_id = Some(commit.id.clone());
        self.save_workspace_state(&workspace)?;

        Ok(id)
    }

    pub fn log(&self) -> Result<Vec<Commit>> {
        let mut commits = Vec::new();
        let mut cursor = self.commit_store.read_head()?;
        let mut seen = HashSet::new();

        while let Some(id) = cursor {
            if !seen.insert(id.clone()) {
                break;
            }
            let commit = match self.commit_store.read_commit(&id) {
                Ok(commit) => commit,
                Err(JetError::ObjectNotFound(_)) => break,
                Err(err) => return Err(err),
            };
            cursor = commit.parent.clone();
            commits.push(commit);
        }

        Ok(commits)
    }

    pub fn head_commit_id(&self) -> Result<Option<String>> {
        self.commit_store.read_head()
    }

    pub fn workspace_view(&self) -> Result<WorkspaceLocalConfig> {
        self.load_workspace_local_config()
    }

    pub fn checkout(&self, commit_id: &str) -> Result<()> {
        self.open_workspace(commit_id, auto_hydrate_on_open_enabled())
    }

    pub fn open_workspace(&self, commit_id: &str, auto_hydrate: bool) -> Result<()> {
        let commit = self.commit_store.read_commit(commit_id)?;
        if commit.files_omitted {
            return Err(JetError::CommitMetadataOnly {
                commit_id: commit_id.to_string(),
            });
        }
        let workspace_view = self.load_workspace_local_config()?;
        let visible_files = commit
            .files
            .iter()
            .filter(|file| self.path_in_view(&workspace_view, &file.path))
            .cloned()
            .collect::<Vec<_>>();
        self.open_workspace_with_files(commit_id, &commit.files, &visible_files, auto_hydrate)
    }

    pub fn open_workspace_with_files(
        &self,
        commit_id: &str,
        target_files: &[CommitFileEntry],
        workspace_files: &[CommitFileEntry],
        auto_hydrate: bool,
    ) -> Result<()> {
        let trace_checkout = trace::checkout_enabled();
        let checkout_started = Instant::now();
        let after_commit_read = Instant::now();
        let mut index = self.load_materialized_index()?;
        let after_index_load = Instant::now();
        self.ensure_no_dirty_workspace(&mut index)?;
        let after_dirty_check = Instant::now();
        let after_view_load = Instant::now();
        let visible_paths = workspace_files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<HashSet<_>>();
        let cleanup_paths =
            self.collect_checkout_cleanup_paths(&index, target_files, &visible_paths);
        self.remove_files_parallel(&cleanup_paths)?;
        let after_cleanup = Instant::now();

        let mut files = std::collections::BTreeMap::new();
        for file in target_files {
            let state = if !visible_paths.contains(file.path.as_str()) {
                MaterializedState::NotInView
            } else if self.can_preserve_hydrated_file(&index, &file.path, &file.file_digest) {
                MaterializedState::Hydrated
            } else {
                MaterializedState::Virtual
            };
            files.insert(
                file.path.clone(),
                MaterializedEntry {
                    state,
                    commit_id: commit_id.to_string(),
                    file_digest: file.file_digest.clone(),
                    size: file.size,
                },
            );
        }
        index.schema_version = 1;
        index.files = files;
        self.save_materialized_index(&index)?;
        self.save_workspace_manifest(&crate::workspace::WorkspaceManifest {
            schema_version: 1,
            commit_id: Some(commit_id.to_string()),
            files: workspace_files.to_vec(),
        })?;
        let after_index_save = Instant::now();

        let mut workspace = self.load_workspace_state()?;
        workspace.current_commit_id = Some(commit_id.to_string());
        self.save_workspace_state(&workspace)?;
        self.commit_store.write_head(commit_id)?;
        let after_state_save = Instant::now();
        if auto_hydrate {
            self.hydrate_default_paths()?;
        }
        if trace_checkout {
            trace::emit_checkout(
                elapsed_ms(after_commit_read.duration_since(checkout_started)),
                elapsed_ms(after_index_load.duration_since(after_commit_read)),
                elapsed_ms(after_dirty_check.duration_since(after_index_load)),
                elapsed_ms(after_view_load.duration_since(after_dirty_check)),
                elapsed_ms(after_cleanup.duration_since(after_view_load)),
                elapsed_ms(after_index_save.duration_since(after_cleanup)),
                elapsed_ms(after_state_save.duration_since(after_index_save)),
                elapsed_ms(checkout_started.elapsed()),
            );
        }
        Ok(())
    }

    pub fn hydrate(&self, paths: &[PathBuf]) -> Result<usize> {
        let trace_hydrate = trace::hydrate_enabled();
        let hydrate_started = Instant::now();
        let commit_id = self.current_workspace_commit_id()?;
        let workspace_files = self.current_workspace_files()?;
        let after_commit_load = Instant::now();
        let checkout_parallel = std::env::var("JET_OPEN_PARALLEL").ok().as_deref() != Some("0");
        let verify_checkout = std::env::var("JET_OPEN_VERIFY").ok().as_deref() == Some("1");
        let mut index = self.load_materialized_index()?;
        self.ensure_no_dirty_workspace(&mut index)?;
        let after_dirty_check = Instant::now();

        let files = workspace_files
            .iter()
            .filter(|file| path_matches(paths, &file.path))
            .filter(|file| {
                index
                    .files
                    .get(&file.path)
                    .map(|entry| {
                        entry.state != MaterializedState::NotInView
                            && entry.state != MaterializedState::Hydrated
                    })
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        let after_filter = Instant::now();
        self.prepare_checkout_dirs(&files.iter().map(|f| (*f).clone()).collect::<Vec<_>>())?;
        let after_prepare_dirs = Instant::now();
        let mut tasks = files
            .iter()
            .map(|file| {
                let ids = file
                    .chunks
                    .iter()
                    .map(|chunk| chunk.id.clone())
                    .collect::<Vec<_>>();
                Ok::<CheckoutTask, JetError>(CheckoutTask {
                    path: file.path.clone(),
                    file_digest: file.file_digest.clone(),
                    chunk_count: file.chunks.len(),
                    size: file.size,
                    chunks: self.object_store.resolve_checkout_chunks(&ids)?,
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let after_resolve = Instant::now();

        if checkout_parallel {
            tasks.sort_by_key(|task| Reverse(task.size));
            self.checkout_files_parallel(tasks, verify_checkout)?;
        } else {
            let mut session = self.object_store.new_checkout_session();
            for task in &tasks {
                self.checkout_file(task, true, verify_checkout, &mut session)?;
            }
        }
        let after_write = Instant::now();

        for file in &files {
            if let Some(entry) = index.files.get_mut(&file.path) {
                entry.state = MaterializedState::Hydrated;
                entry.commit_id = commit_id.clone();
                entry.file_digest = file.file_digest.clone();
                entry.size = file.size;
            }
        }
        self.save_materialized_index(&index)?;
        let after_index_save = Instant::now();
        if trace_hydrate {
            trace::emit_hydrate(
                elapsed_ms(after_commit_load.duration_since(hydrate_started)),
                elapsed_ms(after_dirty_check.duration_since(after_commit_load)),
                elapsed_ms(after_filter.duration_since(after_dirty_check)),
                elapsed_ms(after_prepare_dirs.duration_since(after_filter)),
                elapsed_ms(after_resolve.duration_since(after_prepare_dirs)),
                elapsed_ms(after_write.duration_since(after_resolve)),
                elapsed_ms(after_index_save.duration_since(after_write)),
                files.len(),
                elapsed_ms(after_index_save.duration_since(hydrate_started)),
            );
        }
        Ok(index
            .files
            .values()
            .filter(|entry| entry.state == MaterializedState::Hydrated)
            .count())
    }

    pub fn dehydrate(&self, paths: &[PathBuf]) -> Result<usize> {
        let mut index = self.load_materialized_index()?;
        self.refresh_workspace_states(&mut index)?;

        let mut changed = 0;
        for (path, entry) in &mut index.files {
            if !path_matches(paths, path) {
                continue;
            }
            match entry.state {
                MaterializedState::Dirty => {
                    return Err(JetError::DirtyWorkspaceFile { path: path.clone() });
                }
                MaterializedState::Hydrated => {
                    let absolute = self.root.join(path);
                    if absolute.exists() {
                        fs::remove_file(absolute)?;
                    }
                    entry.state = MaterializedState::Virtual;
                    changed += 1;
                }
                _ => {}
            }
        }

        self.save_materialized_index(&index)?;
        Ok(changed)
    }

    pub fn stats(&self) -> Result<RepoStats> {
        let (object_count, object_bytes) = self.object_store.storage_stats()?;
        Ok(RepoStats {
            object_count,
            object_bytes,
        })
    }

    pub fn workspace_status(&self) -> Result<WorkspaceStatus> {
        let workspace = self.load_workspace_state()?;
        let mut index = self.load_materialized_index()?;
        let workspace_view = self.load_workspace_local_config()?;
        self.refresh_workspace_states(&mut index)?;
        self.save_materialized_index(&index)?;

        let mut status = WorkspaceStatus {
            current_commit_id: workspace.current_commit_id,
            remote_source: workspace.remote_source,
            virtual_count: 0,
            hydrated_count: 0,
            dirty_count: 0,
            pending_count: 0,
            not_in_view_count: 0,
            view_includes: workspace_view.view.include.clone(),
            view_excludes: workspace_view.view.exclude.clone(),
            dirty_paths: Vec::new(),
            pending_paths: Vec::new(),
            not_in_view_paths: Vec::new(),
        };

        for (path, entry) in &index.files {
            match entry.state {
                MaterializedState::NotInView => {
                    status.not_in_view_count += 1;
                    push_status_path(&mut status.not_in_view_paths, path);
                }
                MaterializedState::Virtual => status.virtual_count += 1,
                MaterializedState::Hydrated => status.hydrated_count += 1,
                MaterializedState::Dirty => {
                    status.dirty_count += 1;
                    push_status_path(&mut status.dirty_paths, path);
                }
                MaterializedState::Pending => {
                    status.pending_count += 1;
                    push_status_path(&mut status.pending_paths, path);
                }
            }
        }

        Ok(status)
    }

    pub fn fsck(&self) -> Result<()> {
        self.fsck_with_mode(FsckMode::Quick)
    }

    pub fn fsck_with_mode(&self, mode: FsckMode) -> Result<()> {
        let commits = self.log()?;
        let mut verified_files = HashSet::new();
        for commit in commits {
            if commit.files_omitted {
                continue;
            }
            for file in commit.files {
                let chunk_ids = file
                    .chunks
                    .iter()
                    .map(|chunk| chunk.id.clone())
                    .collect::<Vec<_>>();
                self.object_store.resolve_checkout_chunks(&chunk_ids)?;
                if mode == FsckMode::Quick {
                    continue;
                }
                if verified_files.contains(&file.file_digest) {
                    continue;
                }
                let mut sink = std::io::sink();
                let mut hasher = blake3::Hasher::new();
                self.object_store
                    .write_chunks_in_order(&chunk_ids, &mut sink, &mut hasher)?;
                let digest = hasher.finalize().to_hex().to_string();
                if digest != file.file_digest {
                    return Err(JetError::DigestMismatch { path: file.path });
                }
                verified_files.insert(file.file_digest);
            }
        }

        Ok(())
    }

    fn staging_path(&self) -> PathBuf {
        self.root.join(".jet").join("staging").join("index.bin")
    }

    fn to_candidate(&self, absolute: PathBuf, metadata: fs::Metadata) -> Result<CandidateFile> {
        let relative =
            absolute
                .strip_prefix(&self.root)
                .map_err(|_| JetError::InvalidRepository {
                    path: self.root.clone(),
                })?;
        let relative_path = relative
            .to_str()
            .ok_or(JetError::InvalidUtf8Path)?
            .replace('\\', "/");
        let (modified_unix_secs, modified_nanos) = modified_key(&metadata)?;
        Ok(CandidateFile {
            absolute,
            relative_path,
            metadata_len: metadata.len(),
            modified_unix_secs,
            modified_nanos,
        })
    }

    fn reuse_previous_large_file_chunks(
        &self,
        candidate: &CandidateFile,
        manifest_index: &ManifestIndex,
        data: &[u8],
    ) -> Option<Vec<crate::chunking::ChunkDescriptor>> {
        let previous = manifest_index.entries.get(&candidate.relative_path)?;
        if previous.file.chunks.is_empty() {
            return None;
        }

        let mut chunks = Vec::new();
        let mut resume_offset = 0usize;
        let new_len = data.len() as u64;

        for chunk in &previous.file.chunks {
            let chunk_end = chunk.offset + chunk.len;
            if chunk_end <= new_len {
                chunks.push(crate::chunking::ChunkDescriptor {
                    offset: chunk.offset as usize,
                    length: chunk.len as usize,
                });
                resume_offset = chunk_end as usize;
                continue;
            }

            resume_offset = chunk.offset as usize;
            break;
        }

        if resume_offset < data.len() {
            let suffix = self.chunker.chunk_bytes(&data[resume_offset..]);
            chunks.extend(
                suffix
                    .into_iter()
                    .map(|chunk| crate::chunking::ChunkDescriptor {
                        offset: resume_offset + chunk.offset,
                        length: chunk.length,
                    }),
            );
        }

        if chunks.is_empty() {
            None
        } else {
            Some(chunks)
        }
    }

    fn prepare_large_chunk_refs(
        &self,
        data: &[u8],
        chunks: &[crate::chunking::ChunkDescriptor],
    ) -> (Vec<PreparedChunkRef>, String, u128) {
        let hash_started = Instant::now();
        let mut file_hasher = blake3::Hasher::new();
        let prepared = chunks
            .iter()
            .map(|chunk| {
                let bytes = &data[chunk.offset..chunk.offset + chunk.length];
                file_hasher.update(bytes);
                PreparedChunkRef {
                    id: blake3::hash(bytes).to_hex().to_string(),
                    offset: chunk.offset,
                    length: chunk.length,
                }
            })
            .collect::<Vec<_>>();
        let after_hash = Instant::now();

        (
            prepared,
            file_hasher.finalize().to_hex().to_string(),
            elapsed_ms(after_hash.duration_since(hash_started)),
        )
    }

    fn large_file_matches_previous_chunks(&self, previous: &ManifestEntry, data: &[u8]) -> bool {
        if previous.file.chunks.is_empty() {
            return false;
        }

        let mut expected_offset = 0usize;
        for chunk in &previous.file.chunks {
            let offset = chunk.offset as usize;
            let len = chunk.len as usize;
            if offset != expected_offset {
                return false;
            }
            let end = match offset.checked_add(len) {
                Some(end) => end,
                None => return false,
            };
            if end > data.len() {
                return false;
            }
            let bytes = &data[offset..end];
            let digest = blake3::hash(bytes).to_hex();
            if digest.as_str() != chunk.id {
                return false;
            }
            expected_offset = end;
        }

        expected_offset == data.len()
    }

    fn load_staging(&self) -> Result<StagingIndex> {
        let bin_path = self.staging_path();
        if bin_path.exists() {
            let data = fs::read(bin_path)?;
            return Ok(bincode::deserialize(&data)?);
        }

        let legacy_json_path = self.root.join(".jet").join("staging").join("index.json");
        if !legacy_json_path.exists() {
            return Ok(StagingIndex::default());
        }

        let data = fs::read(legacy_json_path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    fn save_staging(&self, staging: &StagingIndex) -> Result<()> {
        let path = self.staging_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp = path.with_extension("bin.tmp");
        let data = bincode::serialize(staging)?;
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    fn checkout_file(
        &self,
        task: &CheckoutTask,
        allow_chunk_batching: bool,
        verify_checkout: bool,
        session: &mut CheckoutSession,
    ) -> Result<()> {
        let path = self.root.join(&task.path);
        let mut writer = BufWriter::with_capacity(1024 * 1024, fs::File::create(&path)?);
        let mut hasher = verify_checkout.then(blake3::Hasher::new);
        if task.chunk_count == 1 {
            session.write_chunks(&task.chunks, &mut writer, hasher.as_mut())?;
        } else if allow_chunk_batching {
            session.write_chunks(&task.chunks, &mut writer, hasher.as_mut())?;
        } else {
            for chunk in &task.chunks {
                session.write_chunks(std::slice::from_ref(chunk), &mut writer, hasher.as_mut())?;
            }
        }
        writer.flush()?;

        if let Some(hasher) = hasher {
            let digest = hasher.finalize().to_hex().to_string();
            if digest != task.file_digest {
                return Err(JetError::DigestMismatch {
                    path: task.path.clone(),
                });
            }
        }
        Ok(())
    }

    fn prepare_checkout_dirs(&self, files: &[CommitFileEntry]) -> Result<()> {
        let mut parents = HashSet::new();
        for file in files {
            if let Some(parent) = self.root.join(&file.path).parent() {
                parents.insert(parent.to_path_buf());
            }
        }
        let mut parents = parents.into_iter().collect::<Vec<_>>();
        parents.sort();
        for parent in parents {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    fn checkout_files_parallel(
        &self,
        files: Vec<CheckoutTask>,
        verify_checkout: bool,
    ) -> Result<()> {
        let worker_count = std::env::var("JET_OPEN_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or_else(default_open_threads)
            .min(files.len().max(1));
        let next = AtomicUsize::new(0);
        let first_error = Mutex::new(None);

        thread::scope(|scope| {
            for _ in 0..worker_count {
                let repo = self.clone();
                let files = &files;
                let next = &next;
                let first_error = &first_error;
                let verify_checkout = verify_checkout;
                scope.spawn(move || {
                    let mut session = repo.object_store.new_checkout_session();
                    loop {
                        if first_error.lock().expect("lock").is_some() {
                            break;
                        }
                        let idx = next.fetch_add(1, Ordering::Relaxed);
                        if idx >= files.len() {
                            break;
                        }
                        if let Err(err) =
                            repo.checkout_file(&files[idx], true, verify_checkout, &mut session)
                        {
                            let mut slot = first_error.lock().expect("lock");
                            if slot.is_none() {
                                *slot = Some(err);
                            }
                            break;
                        }
                    }
                });
            }
        });

        if let Some(err) = first_error.into_inner().expect("lock") {
            return Err(err);
        }
        Ok(())
    }

    pub(crate) fn load_workspace_state(&self) -> Result<WorkspaceState> {
        load_workspace_state(&self.root, &self.config)
    }

    pub(crate) fn save_workspace_state(&self, state: &WorkspaceState) -> Result<()> {
        save_workspace_state(&self.root, state)
    }

    pub fn load_workspace_manifest(&self) -> Result<crate::workspace::WorkspaceManifest> {
        load_workspace_manifest(&self.root)
    }

    pub fn save_workspace_manifest(
        &self,
        manifest: &crate::workspace::WorkspaceManifest,
    ) -> Result<()> {
        save_workspace_manifest(&self.root, manifest)
    }

    fn load_materialized_index(&self) -> Result<MaterializedIndex> {
        load_materialized_index(&self.root)
    }

    fn load_workspace_local_config(&self) -> Result<WorkspaceLocalConfig> {
        load_workspace_local_config(&self.root)
    }

    fn save_materialized_index(&self, index: &MaterializedIndex) -> Result<()> {
        save_materialized_index(&self.root, index)
    }

    pub fn save_workspace_view(&self, config: &WorkspaceLocalConfig) -> Result<()> {
        save_workspace_local_config(&self.root, config)
    }

    fn current_workspace_commit_id(&self) -> Result<String> {
        let workspace = self.load_workspace_state()?;
        workspace
            .current_commit_id
            .or(self.commit_store.read_head()?)
            .ok_or(JetError::NoWorkspaceCommit)
    }

    fn current_workspace_files(&self) -> Result<Vec<CommitFileEntry>> {
        let commit_id = self.current_workspace_commit_id()?;
        let manifest = self.load_workspace_manifest()?;
        if manifest.commit_id.as_deref() == Some(commit_id.as_str()) {
            return Ok(manifest.files);
        }
        Ok(self.commit_store.read_commit(&commit_id)?.files)
    }

    fn ensure_no_dirty_workspace(&self, index: &mut MaterializedIndex) -> Result<()> {
        self.refresh_workspace_states(index)?;
        if let Some((path, _)) = index
            .files
            .iter()
            .find(|(_, entry)| entry.state == MaterializedState::Dirty)
        {
            return Err(JetError::DirtyWorkspaceFile { path: path.clone() });
        }
        Ok(())
    }

    pub fn ensure_clean_workspace(&self) -> Result<()> {
        let mut index = self.load_materialized_index()?;
        self.ensure_no_dirty_workspace(&mut index)
    }

    pub fn workspace_remote_source(&self) -> Result<Option<String>> {
        Ok(self.load_workspace_state()?.remote_source)
    }

    pub fn set_workspace_remote_source(&self, remote_source: Option<String>) -> Result<()> {
        let mut workspace = self.load_workspace_state()?;
        workspace.remote_source = remote_source;
        self.save_workspace_state(&workspace)
    }

    fn refresh_workspace_states(&self, index: &mut MaterializedIndex) -> Result<()> {
        let manifest_index = ManifestIndex::load(&self.root)?;
        for (path, entry) in &mut index.files {
            if entry.state != MaterializedState::Hydrated && entry.state != MaterializedState::Dirty
            {
                continue;
            }
            let absolute = self.root.join(path);
            if !absolute.exists() {
                entry.state = MaterializedState::Virtual;
                continue;
            }

            let metadata = fs::metadata(&absolute)?;
            let (modified_unix_secs, modified_nanos) = modified_key(&metadata)?;
            if let Some(cached) = manifest_index.get_if_unchanged(
                path,
                metadata.len(),
                modified_unix_secs,
                modified_nanos,
            ) && cached.file_digest == entry.file_digest
            {
                entry.state = MaterializedState::Hydrated;
                continue;
            }

            let digest = blake3::hash(&fs::read(&absolute)?).to_hex().to_string();
            entry.state = if digest == entry.file_digest {
                MaterializedState::Hydrated
            } else {
                MaterializedState::Dirty
            };
        }
        Ok(())
    }

    fn collect_checkout_cleanup_paths(
        &self,
        index: &MaterializedIndex,
        target_files: &[CommitFileEntry],
        visible_paths: &HashSet<&str>,
    ) -> Vec<String> {
        let target_files = target_files
            .iter()
            .map(|file| (file.path.as_str(), file))
            .collect::<HashMap<_, _>>();
        let mut paths = HashSet::new();

        for (path, entry) in &index.files {
            if entry.state != MaterializedState::Hydrated {
                continue;
            }

            let Some(target) = target_files.get(path.as_str()) else {
                paths.insert(path.clone());
                continue;
            };

            if !visible_paths.contains(path.as_str()) || entry.file_digest != target.file_digest {
                paths.insert(path.clone());
            }
        }

        for file in target_files.values() {
            if !visible_paths.contains(file.path.as_str())
                || !self.can_preserve_hydrated_file(index, &file.path, &file.file_digest)
            {
                paths.insert(file.path.clone());
            }
        }

        paths.into_iter().collect()
    }

    fn can_preserve_hydrated_file(
        &self,
        index: &MaterializedIndex,
        path: &str,
        target_digest: &str,
    ) -> bool {
        matches!(
            index.files.get(path),
            Some(entry)
                if entry.state == MaterializedState::Hydrated
                    && entry.file_digest == target_digest
                    && self.root.join(path).exists()
        )
    }

    fn remove_file_if_exists(&self, path: &str) -> Result<()> {
        let absolute = self.root.join(path);
        if absolute.exists() {
            fs::remove_file(absolute)?;
        }
        Ok(())
    }

    fn remove_files_parallel(&self, paths: &[String]) -> Result<()> {
        paths
            .par_iter()
            .try_for_each(|path| self.remove_file_if_exists(path))
    }

    fn hydrate_default_paths(&self) -> Result<()> {
        let files = self.current_workspace_files()?;
        let workspace_view = self.load_workspace_local_config()?;
        let default_files = files
            .iter()
            .filter(|file| self.path_in_view(&workspace_view, &file.path))
            .filter(|file| {
                should_auto_hydrate_with_patterns(
                    &file.path,
                    file.size,
                    &self.config.workspace.hot_paths,
                    self.config.workspace.max_hot_file_bytes,
                )
            })
            .map(|file| PathBuf::from(file.path.clone()))
            .collect::<Vec<_>>();

        if default_files.is_empty() {
            return Ok(());
        }

        let _ = self.hydrate(&default_files)?;
        Ok(())
    }

    fn sync_materialized_after_commit(&self, commit: &Commit) -> Result<()> {
        let workspace_view = self.load_workspace_local_config()?;
        let mut index = self.load_materialized_index()?;

        for file in &commit.files {
            let state = if !self.path_in_view(&workspace_view, &file.path) {
                MaterializedState::NotInView
            } else if self.root.join(&file.path).exists() {
                MaterializedState::Hydrated
            } else {
                MaterializedState::Virtual
            };
            index.files.insert(
                file.path.clone(),
                MaterializedEntry {
                    state,
                    commit_id: commit.id.clone(),
                    file_digest: file.file_digest.clone(),
                    size: file.size,
                },
            );
        }

        let visible_files = commit
            .files
            .iter()
            .filter(|file| self.path_in_view(&workspace_view, &file.path))
            .cloned()
            .collect::<Vec<_>>();
        self.save_workspace_manifest(&crate::workspace::WorkspaceManifest {
            schema_version: 1,
            commit_id: Some(commit.id.clone()),
            files: visible_files,
        })?;
        self.save_materialized_index(&index)?;
        Ok(())
    }

    fn path_in_view(&self, config: &WorkspaceLocalConfig, path: &str) -> bool {
        let included = if config.view.include.is_empty() {
            true
        } else {
            config
                .view
                .include
                .iter()
                .any(|pattern| workspace_pattern_matches(pattern, path))
        };

        if !included {
            return false;
        }

        !config
            .view
            .exclude
            .iter()
            .any(|pattern| workspace_pattern_matches(pattern, path))
    }
}

#[derive(Debug)]
struct ProcessedFile {
    entry: CommitFileEntry,
    metadata_len: u64,
    modified_unix_secs: u64,
    modified_nanos: u32,
    was_reused: bool,
    trace: AddFileTrace,
}

#[derive(Debug, Default, Clone, Copy)]
struct AddFileTrace {
    small_file_count: usize,
    large_file_count: usize,
    reused_unchanged_count: usize,
    reused_same_content_count: usize,
    cache_hit_count: usize,
    reused_boundary_count: usize,
    small_read_ms: u128,
    small_store_ms: u128,
    large_digest_ms: u128,
    large_chunking_ms: u128,
    large_store_ms: u128,
}

#[derive(Debug, Clone)]
struct PreparedChunkRef {
    id: String,
    offset: usize,
    length: usize,
}

fn maybe_init_rayon_pool() {
    if let Ok(v) = std::env::var("JET_RAYON_THREADS")
        && let Ok(threads) = v.parse::<usize>()
        && threads > 0
    {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global();
    }
}

fn default_open_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn path_matches(paths: &[PathBuf], file_path: &str) -> bool {
    if paths.is_empty() {
        return true;
    }

    paths.iter().any(|path| {
        let normalized = path.to_string_lossy().replace('\\', "/");
        let normalized = normalized.trim_end_matches('/');
        normalized == "."
            || normalized.is_empty()
            || file_path == normalized
            || file_path.starts_with(&format!("{normalized}/"))
    })
}

pub fn should_auto_hydrate(path: &str, size: u64) -> bool {
    let default_hot_paths = ["code/...", "config/..."];
    should_auto_hydrate_with_patterns(path, size, &default_hot_paths, 256 * 1024)
}

pub fn should_auto_hydrate_with_patterns(
    path: &str,
    size: u64,
    hot_paths: &[impl AsRef<str>],
    max_hot_file_bytes: u64,
) -> bool {
    if size > max_hot_file_bytes {
        return false;
    }

    hot_paths
        .iter()
        .any(|pattern| workspace_pattern_matches(pattern.as_ref(), path))
}

fn auto_hydrate_on_open_enabled() -> bool {
    std::env::var("JET_AUTO_HYDRATE_ON_OPEN").ok().as_deref() != Some("0")
}

fn elapsed_ms(duration: std::time::Duration) -> u128 {
    duration.as_millis()
}

fn push_status_path(paths: &mut Vec<String>, path: &str) {
    const STATUS_PATH_SAMPLE_LIMIT: usize = 10;

    if paths.len() < STATUS_PATH_SAMPLE_LIMIT {
        paths.push(path.to_string());
    }
}

fn workspace_pattern_matches(pattern: &str, path: &str) -> bool {
    let pattern = pattern.trim().replace('\\', "/");
    let pattern = pattern.trim_end_matches('/');
    if pattern.is_empty() || pattern == "..." {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix("/...") {
        return path == prefix || path.starts_with(&format!("{prefix}/"));
    }

    if pattern.contains('*') {
        if let Some((dir, suffix)) = pattern.rsplit_once('/') {
            return (path == dir || path.starts_with(&format!("{dir}/")))
                && path
                    .rsplit('/')
                    .next()
                    .map(|name| simple_glob_matches(suffix, name))
                    .unwrap_or(false);
        }
        return path
            .rsplit('/')
            .next()
            .map(|name| simple_glob_matches(&pattern, name))
            .unwrap_or(false);
    }

    path == pattern || path.starts_with(&format!("{pattern}/"))
}

fn simple_glob_matches(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return name.rsplit('.').next() == Some(suffix);
    }
    pattern == name
}

fn collect_dir_file_entries(root: &Path, files: &mut Vec<(PathBuf, fs::Metadata)>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_dir_file_entries(&path, files)?;
        } else {
            files.push((path, entry.metadata()?));
        }
    }
    Ok(())
}

fn commit_id(parent: &Option<String>, message: &str, ts: i64, files: &[CommitFileEntry]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(parent.as_deref().unwrap_or("root").as_bytes());
    hasher.update(message.as_bytes());
    hasher.update(ts.to_string().as_bytes());

    for file in files {
        hasher.update(file.path.as_bytes());
        hasher.update(file.file_digest.as_bytes());
        hasher.update(file.size.to_string().as_bytes());
    }

    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::chunk_cache::ChunkCache;
    use crate::chunking::Chunker;
    use crate::commit_store::{CommitChunkRef, CommitFileEntry};
    use crate::engine::JetRepository;
    use crate::error::JetError;
    use crate::manifest_index::{ManifestEntry, ManifestIndex};
    use crate::repo::init_repo;

    #[test]
    fn add_commit_checkout_round_trip() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");

        let file = dir.path().join("asset.bin");
        fs::write(&file, vec![42_u8; 256 * 1024]).expect("write");

        let repo = JetRepository::open(dir.path()).expect("open");
        repo.add_paths(&[file.clone()]).expect("add");
        let commit_id = repo.commit("initial", "tester").expect("commit");

        fs::write(&file, b"corrupted").expect("rewrite");
        let err = repo
            .checkout(&commit_id)
            .expect_err("dirty checkout should fail");
        assert!(matches!(err, JetError::DirtyWorkspaceFile { .. }));
        repo.add_paths(std::slice::from_ref(&file))
            .expect("restage");
        repo.commit("corrupted", "tester").expect("commit dirty");
        repo.checkout(&commit_id).expect("checkout");
        assert!(!file.exists());
        repo.hydrate(&[]).expect("hydrate");

        let restored = fs::read(&file).expect("read");
        assert_eq!(restored, vec![42_u8; 256 * 1024]);
    }

    #[test]
    fn dehydrate_removes_clean_hydrated_file() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");

        let file = dir.path().join("asset.bin");
        fs::write(&file, vec![7_u8; 64 * 1024]).expect("write");

        let repo = JetRepository::open(dir.path()).expect("open");
        repo.add_paths(&[file.clone()]).expect("add");
        let commit_id = repo.commit("initial", "tester").expect("commit");
        repo.checkout(&commit_id).expect("checkout");
        repo.hydrate(&[]).expect("hydrate");

        repo.dehydrate(&[]).expect("dehydrate");
        assert!(!file.exists());
    }

    #[test]
    fn checkout_respects_workspace_view_include_exclude() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");
        fs::write(
            dir.path().join(".jet").join("workspace.local.toml"),
            "[view]\ninclude = [\"assets/...\"]\nexclude = [\"assets/excluded/...\"]\n",
        )
        .expect("workspace local");

        let include_dir = dir.path().join("assets").join("included");
        let exclude_dir = dir.path().join("assets").join("excluded");
        fs::create_dir_all(&include_dir).expect("mkdir include");
        fs::create_dir_all(&exclude_dir).expect("mkdir exclude");
        let include_file = include_dir.join("kept.bin");
        let exclude_file = exclude_dir.join("skipped.bin");
        fs::write(&include_file, b"keep").expect("write include");
        fs::write(&exclude_file, b"skip").expect("write exclude");

        let repo = JetRepository::open(dir.path()).expect("open");
        repo.add_paths(&[dir.path().join("assets")]).expect("add");
        let commit_id = repo.commit("initial", "tester").expect("commit");

        repo.checkout(&commit_id).expect("checkout");
        let status = repo.workspace_status().expect("status");
        assert_eq!(status.virtual_count + status.hydrated_count, 1);
        assert_eq!(status.not_in_view_count, 1);
    }

    #[test]
    fn checkout_preserves_unchanged_hydrated_files() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");

        let assets = dir.path().join("assets");
        fs::create_dir_all(&assets).expect("mkdir");
        let kept = assets.join("kept.bin");
        let changed = assets.join("changed.bin");
        fs::write(&kept, b"same").expect("write kept");
        fs::write(&changed, b"v1").expect("write changed");

        let repo = JetRepository::open(dir.path()).expect("open");
        repo.add_paths(std::slice::from_ref(&assets)).expect("add");
        let commit1 = repo.commit("initial", "tester").expect("commit1");
        repo.checkout(&commit1).expect("checkout1");
        repo.hydrate(&[]).expect("hydrate1");

        fs::write(&changed, b"v2").expect("rewrite changed");
        repo.add_paths(std::slice::from_ref(&assets))
            .expect("add assets");
        let _commit2 = repo.commit("second", "tester").expect("commit2");
        repo.checkout(&commit1).expect("checkout2");

        assert!(kept.exists());
        assert!(!changed.exists());
        let status = repo.workspace_status().expect("status");
        assert_eq!(status.hydrated_count, 1);
        assert_eq!(status.virtual_count, 1);
    }

    #[test]
    fn commit_updates_workspace_current_commit_id() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");

        let file = dir.path().join("code.rs");
        fs::write(&file, "fn main() {}\n").expect("write");

        let repo = JetRepository::open(dir.path()).expect("open");
        repo.add_paths(std::slice::from_ref(&file)).expect("add");
        let commit_id = repo.commit("initial", "tester").expect("commit");

        let status = repo.workspace_status().expect("status");
        assert_eq!(
            status.current_commit_id.as_deref(),
            Some(commit_id.as_str())
        );
        assert_eq!(
            repo.head_commit_id().expect("head").as_deref(),
            Some(commit_id.as_str())
        );
    }

    #[test]
    fn reuses_large_file_prefix_chunks_when_file_grows() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");
        let repo = JetRepository::open(dir.path()).expect("open");

        let original = vec![1_u8; (8 * 1024 * 1024) + 1234];
        let grown = [original.as_slice(), &[2_u8; 4096]].concat();
        let original_chunks = repo.chunker.chunk_bytes(&original);

        let mut manifest = ManifestIndex::default();
        manifest.entries.insert(
            "assets/large.bin".to_string(),
            ManifestEntry {
                path: "assets/large.bin".to_string(),
                size: original.len() as u64,
                modified_unix_secs: 0,
                modified_nanos: 0,
                file: CommitFileEntry {
                    path: "assets/large.bin".to_string(),
                    size: original.len() as u64,
                    file_digest: "old".to_string(),
                    chunks: original_chunks
                        .iter()
                        .map(|chunk| CommitChunkRef {
                            id: String::new(),
                            offset: chunk.offset as u64,
                            len: chunk.length as u64,
                            raw_len: chunk.length as u64,
                        })
                        .collect(),
                },
            },
        );

        let candidate = super::CandidateFile {
            absolute: dir.path().join("assets/large.bin"),
            relative_path: "assets/large.bin".to_string(),
            metadata_len: grown.len() as u64,
            modified_unix_secs: 0,
            modified_nanos: 0,
        };

        let reused = repo
            .reuse_previous_large_file_chunks(&candidate, &manifest, &grown)
            .expect("reused chunks");

        assert!(reused.len() >= original_chunks.len());
        assert_eq!(
            reused[..original_chunks.len()]
                .iter()
                .map(|chunk| (chunk.offset, chunk.length))
                .collect::<Vec<_>>(),
            original_chunks
                .iter()
                .map(|chunk| (chunk.offset, chunk.length))
                .collect::<Vec<_>>()
        );
        let total: usize = reused.iter().map(|chunk| chunk.length).sum();
        assert_eq!(total, grown.len());
    }

    #[test]
    fn reuses_large_file_prefix_chunks_when_file_shrinks() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");
        let repo = JetRepository::open(dir.path()).expect("open");

        let original = vec![3_u8; (8 * 1024 * 1024) + 8192];
        let shrunk = original[..original.len() - 4096].to_vec();
        let original_chunks = repo.chunker.chunk_bytes(&original);

        let mut manifest = ManifestIndex::default();
        manifest.entries.insert(
            "assets/large.bin".to_string(),
            ManifestEntry {
                path: "assets/large.bin".to_string(),
                size: original.len() as u64,
                modified_unix_secs: 0,
                modified_nanos: 0,
                file: CommitFileEntry {
                    path: "assets/large.bin".to_string(),
                    size: original.len() as u64,
                    file_digest: "old".to_string(),
                    chunks: original_chunks
                        .iter()
                        .map(|chunk| CommitChunkRef {
                            id: String::new(),
                            offset: chunk.offset as u64,
                            len: chunk.length as u64,
                            raw_len: chunk.length as u64,
                        })
                        .collect(),
                },
            },
        );

        let candidate = super::CandidateFile {
            absolute: dir.path().join("assets/large.bin"),
            relative_path: "assets/large.bin".to_string(),
            metadata_len: shrunk.len() as u64,
            modified_unix_secs: 0,
            modified_nanos: 0,
        };

        let reused = repo
            .reuse_previous_large_file_chunks(&candidate, &manifest, &shrunk)
            .expect("reused chunks");

        let total: usize = reused.iter().map(|chunk| chunk.length).sum();
        assert_eq!(total, shrunk.len());
        for pair in reused.windows(2) {
            assert_eq!(pair[0].offset + pair[0].length, pair[1].offset);
        }
    }

    #[test]
    fn process_file_reuses_same_large_content_before_rechunking() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init");
        let repo = JetRepository::open(dir.path()).expect("open");

        let file = dir.path().join("assets").join("large.bin");
        fs::create_dir_all(file.parent().expect("parent")).expect("mkdirs");
        let data = vec![7_u8; (8 * 1024 * 1024) + 321];
        fs::write(&file, &data).expect("write");

        let chunk_descriptors = repo.chunker.chunk_bytes(&data);
        let chunk_refs = chunk_descriptors
            .iter()
            .map(|chunk| CommitChunkRef {
                id: blake3::hash(&data[chunk.offset..chunk.offset + chunk.length])
                    .to_hex()
                    .to_string(),
                offset: chunk.offset as u64,
                len: chunk.length as u64,
                raw_len: chunk.length as u64,
            })
            .collect::<Vec<_>>();
        let file_digest = blake3::hash(&data).to_hex().to_string();

        let mut manifest = ManifestIndex::default();
        manifest.entries.insert(
            "assets/large.bin".to_string(),
            ManifestEntry {
                path: "assets/large.bin".to_string(),
                size: data.len() as u64,
                modified_unix_secs: 1,
                modified_nanos: 0,
                file: CommitFileEntry {
                    path: "assets/large.bin".to_string(),
                    size: data.len() as u64,
                    file_digest: file_digest.clone(),
                    chunks: chunk_refs.clone(),
                },
            },
        );

        let metadata = fs::metadata(&file).expect("metadata");
        let candidate = repo
            .to_candidate(file.clone(), metadata)
            .expect("candidate");
        let chunk_cache = ChunkCache::default();
        let live_chunk_cache = Arc::new(Mutex::new(HashMap::new()));

        let processed = repo
            .process_file(&candidate, &manifest, &chunk_cache, &live_chunk_cache)
            .expect("process");

        assert_eq!(processed.entry.file_digest, file_digest);
        assert_eq!(processed.entry.chunks.len(), chunk_refs.len());
        assert_eq!(
            processed
                .entry
                .chunks
                .iter()
                .map(|chunk| (&chunk.id, chunk.offset, chunk.len, chunk.raw_len))
                .collect::<Vec<_>>(),
            chunk_refs
                .iter()
                .map(|chunk| (&chunk.id, chunk.offset, chunk.len, chunk.raw_len))
                .collect::<Vec<_>>()
        );
        assert_eq!(processed.trace.reused_same_content_count, 1);
        assert_eq!(processed.trace.large_chunking_ms, 0);
    }
}

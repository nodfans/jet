use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::error::{JetError, Result};

const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;
const SEGMENT_INDEX_ID_BYTES: usize = 64;
const SEGMENT_INDEX_RECORD_BYTES: usize = SEGMENT_INDEX_ID_BYTES + (8 * 4) + 1;

#[derive(Debug, Clone)]
pub struct StoreChunkResult {
    pub id: String,
    pub was_new: bool,
    pub raw_size: u64,
    pub compressed_size: u64,
}

pub trait ObjectStore {
    fn put_chunk(&self, bytes: &[u8]) -> Result<StoreChunkResult> {
        let id = blake3::hash(bytes).to_hex().to_string();
        self.put_chunk_with_id(&id, bytes)
    }

    fn put_chunk_with_id(&self, id: &str, bytes: &[u8]) -> Result<StoreChunkResult>;
    fn get_chunk(&self, id: &str) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub struct FsObjectStore {
    inner: Arc<FsObjectStoreInner>,
}

#[derive(Debug, Clone)]
pub enum CheckoutChunkSource {
    Segment {
        id: String,
        segment_id: u64,
        offset: u64,
        stored_len: u64,
        compressed: bool,
    },
    Legacy(PathBuf),
}

#[derive(Debug)]
pub struct CheckoutSession {
    store: FsObjectStore,
    segment_files: HashMap<u64, File>,
}

#[derive(Debug)]
struct FsObjectStoreInner {
    objects_root: PathBuf,
    segments_root: PathBuf,
    index_file_path: PathBuf,
    legacy_objects_present: bool,
    compression_enabled: bool,
    compression_level: i32,
    fsync_each_object: bool,
    max_segment_size_bytes: u64,
    segment_index: RwLock<HashMap<String, SegmentIndexEntry>>,
    state: Mutex<SegmentStoreState>,
}

#[derive(Debug)]
struct SegmentStoreState {
    current_segment_id: u64,
    current_size: u64,
    segment_file: File,
    index_file: File,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentIndexEntry {
    id: String,
    segment_id: u64,
    offset: u64,
    stored_len: u64,
    raw_len: u64,
    compressed: bool,
}

impl FsObjectStore {
    pub fn new(
        repo_root: impl AsRef<Path>,
        compression_enabled: bool,
        compression_level: i32,
    ) -> Result<Self> {
        let jet_root = repo_root.as_ref().join(".jet");
        let objects_root = jet_root.join("objects");
        let segments_root = jet_root.join("segments");
        let index_root = jet_root.join("index");
        let index_file_path = index_root.join("segments.idx");
        let legacy_index_file_path = index_root.join("segments.jsonl");

        fs::create_dir_all(&objects_root)?;
        fs::create_dir_all(&segments_root)?;
        fs::create_dir_all(&index_root)?;

        let segment_index = load_segment_index(&index_file_path, &legacy_index_file_path)?;
        let state = open_segment_state(&segments_root, &index_file_path, &segment_index)?;
        let legacy_objects_present = detect_legacy_objects(&objects_root)?;

        Ok(Self {
            inner: Arc::new(FsObjectStoreInner {
                objects_root,
                segments_root,
                index_file_path,
                legacy_objects_present,
                compression_enabled,
                compression_level,
                fsync_each_object: std::env::var("JET_FSYNC_EACH_OBJECT").ok().as_deref()
                    == Some("1"),
                max_segment_size_bytes: std::env::var("JET_SEGMENT_SIZE_MB")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|mb| mb * 1024 * 1024)
                    .unwrap_or(DEFAULT_SEGMENT_SIZE_BYTES),
                segment_index: RwLock::new(segment_index),
                state: Mutex::new(state),
            }),
        })
    }

    pub fn storage_stats(&self) -> Result<(u64, u64)> {
        let object_count = self.inner.segment_index.read().map_err(lock_err)?.len() as u64;
        let mut total_bytes = 0_u64;

        if self.inner.segments_root.exists() {
            for path in walk_dir_files(&self.inner.segments_root)? {
                total_bytes += fs::metadata(path)?.len();
            }
        }

        if total_bytes == 0 && self.inner.objects_root.exists() {
            let mut legacy_count = 0_u64;
            for path in walk_dir_files(&self.inner.objects_root)? {
                legacy_count += 1;
                total_bytes += fs::metadata(path)?.len();
            }
            return Ok((legacy_count, total_bytes));
        }

        Ok((object_count, total_bytes))
    }

    pub fn new_checkout_session(&self) -> CheckoutSession {
        CheckoutSession {
            store: self.clone(),
            segment_files: HashMap::new(),
        }
    }

    pub fn resolve_checkout_chunks(&self, ids: &[String]) -> Result<Vec<CheckoutChunkSource>> {
        let index = self.inner.segment_index.read().map_err(lock_err)?;
        let mut chunks = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(entry) = index.get(id) {
                chunks.push(CheckoutChunkSource::Segment {
                    id: entry.id.clone(),
                    segment_id: entry.segment_id,
                    offset: entry.offset,
                    stored_len: entry.stored_len,
                    compressed: entry.compressed,
                });
            } else {
                let Some(path) = self.legacy_object_path(id) else {
                    return Err(JetError::ObjectNotFound(id.clone()));
                };
                chunks.push(CheckoutChunkSource::Legacy(path));
            }
        }
        Ok(chunks)
    }

    pub fn has_chunk_id(&self, id: &str) -> Result<bool> {
        if self
            .inner
            .segment_index
            .read()
            .map_err(lock_err)?
            .contains_key(id)
        {
            return Ok(true);
        }
        Ok(self.legacy_object_path(id).is_some())
    }

    pub fn ensure_chunk_with_id(&self, id: &str, bytes: &[u8]) -> Result<()> {
        if self.has_chunk_id(id)? {
            return Ok(());
        }

        let stored = if self.inner.compression_enabled {
            let mut out = Vec::new();
            let mut encoder = zstd::Encoder::new(&mut out, self.inner.compression_level)?;
            encoder.write_all(bytes)?;
            encoder.finish()?;
            out
        } else {
            bytes.to_vec()
        };

        let entry = {
            let mut state = self.inner.state.lock().map_err(lock_err)?;
            if self
                .inner
                .segment_index
                .read()
                .map_err(lock_err)?
                .contains_key(id)
            {
                return Ok(());
            }
            if self.legacy_object_path(id).is_some() {
                return Ok(());
            }
            if state.current_size + stored.len() as u64 > self.inner.max_segment_size_bytes {
                rotate_segment(
                    &self.inner.segments_root,
                    &self.inner.index_file_path,
                    &mut state,
                )?;
            }

            let offset = state.current_size;
            state.segment_file.write_all(&stored)?;
            if self.inner.fsync_each_object {
                state.segment_file.sync_data()?;
            }

            state.current_size += stored.len() as u64;

            let entry = SegmentIndexEntry {
                id: id.to_string(),
                segment_id: state.current_segment_id,
                offset,
                stored_len: stored.len() as u64,
                raw_len: bytes.len() as u64,
                compressed: self.inner.compression_enabled,
            };

            write_segment_index_record(&mut state.index_file, &entry)?;
            if self.inner.fsync_each_object {
                state.index_file.sync_data()?;
            }

            entry
        };

        self.inner
            .segment_index
            .write()
            .map_err(lock_err)?
            .insert(id.to_string(), entry);

        Ok(())
    }

    pub fn ensure_chunks_with_ids<'a>(&self, chunks: &[(&'a str, &'a [u8])]) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        let pending = {
            let index = self.inner.segment_index.read().map_err(lock_err)?;
            let mut pending = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for (id, bytes) in chunks {
                if !seen.insert(*id) {
                    continue;
                }
                if index.contains_key(*id) {
                    continue;
                }
                if self.legacy_object_path(id).is_some() {
                    continue;
                }
                let stored = if self.inner.compression_enabled {
                    let mut out = Vec::new();
                    let mut encoder = zstd::Encoder::new(&mut out, self.inner.compression_level)?;
                    encoder.write_all(bytes)?;
                    encoder.finish()?;
                    out
                } else {
                    bytes.to_vec()
                };
                pending.push(((*id).to_string(), bytes.len() as u64, stored));
            }
            pending
        };

        if pending.is_empty() {
            return Ok(());
        }

        let entries = {
            let mut state = self.inner.state.lock().map_err(lock_err)?;
            let mut entries = Vec::with_capacity(pending.len());
            for (id, raw_len, stored) in pending {
                if self
                    .inner
                    .segment_index
                    .read()
                    .map_err(lock_err)?
                    .contains_key(&id)
                {
                    continue;
                }
                if self.legacy_object_path(&id).is_some() {
                    continue;
                }
                if state.current_size + stored.len() as u64 > self.inner.max_segment_size_bytes {
                    rotate_segment(
                        &self.inner.segments_root,
                        &self.inner.index_file_path,
                        &mut state,
                    )?;
                }

                let offset = state.current_size;
                state.segment_file.write_all(&stored)?;
                if self.inner.fsync_each_object {
                    state.segment_file.sync_data()?;
                }
                state.current_size += stored.len() as u64;

                let entry = SegmentIndexEntry {
                    id,
                    segment_id: state.current_segment_id,
                    offset,
                    stored_len: stored.len() as u64,
                    raw_len,
                    compressed: self.inner.compression_enabled,
                };
                write_segment_index_record(&mut state.index_file, &entry)?;
                if self.inner.fsync_each_object {
                    state.index_file.sync_data()?;
                }
                entries.push(entry);
            }
            entries
        };

        let mut index = self.inner.segment_index.write().map_err(lock_err)?;
        for entry in entries {
            index.insert(entry.id.clone(), entry);
        }
        Ok(())
    }

    pub fn write_chunk<W: Write>(
        &self,
        id: &str,
        writer: &mut W,
        hasher: &mut blake3::Hasher,
    ) -> Result<()> {
        let entry = self
            .inner
            .segment_index
            .read()
            .map_err(lock_err)?
            .get(id)
            .cloned();

        if let Some(entry) = entry {
            let mut files = HashMap::new();
            return self.write_segment_entry(&entry, &mut files, writer, hasher);
        }

        let Some(path) = self.legacy_object_path(id) else {
            return Err(JetError::ObjectNotFound(id.to_string()));
        };
        self.write_legacy_path(&path, writer, Some(hasher))
    }

    pub fn write_chunks_in_order<W: Write>(
        &self,
        ids: &[String],
        writer: &mut W,
        hasher: &mut blake3::Hasher,
    ) -> Result<()> {
        let index = self.inner.segment_index.read().map_err(lock_err)?;
        let mut ordered = Vec::with_capacity(ids.len());

        for (idx, id) in ids.iter().enumerate() {
            if let Some(entry) = index.get(id).cloned() {
                ordered.push((idx, OrderedChunk::Segment(entry)));
            } else {
                let Some(path) = self.legacy_object_path(id) else {
                    return Err(JetError::ObjectNotFound(id.clone()));
                };
                ordered.push((idx, OrderedChunk::Legacy(path)));
            }
        }
        drop(index);

        ordered.sort_by_key(|(idx, _)| *idx);
        let mut segment_files = HashMap::new();
        for (_, chunk) in ordered {
            match chunk {
                OrderedChunk::Segment(entry) => {
                    self.write_segment_entry(&entry, &mut segment_files, writer, hasher)?
                }
                OrderedChunk::Legacy(path) => {
                    self.write_legacy_path(&path, writer, Some(hasher))?
                }
            }
        }

        Ok(())
    }

    fn zstd_path(&self, id: &str) -> PathBuf {
        let prefix = &id[0..2];
        self.inner
            .objects_root
            .join(prefix)
            .join(format!("{id}.obj.zst"))
    }

    fn raw_path(&self, id: &str) -> PathBuf {
        let prefix = &id[0..2];
        self.inner
            .objects_root
            .join(prefix)
            .join(format!("{id}.obj"))
    }

    fn legacy_object_path(&self, id: &str) -> Option<PathBuf> {
        if !self.inner.legacy_objects_present {
            return None;
        }
        let raw = self.raw_path(id);
        if raw.exists() {
            return Some(raw);
        }
        let zstd = self.zstd_path(id);
        if zstd.exists() {
            return Some(zstd);
        }
        None
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.inner
            .segments_root
            .join(format!("{segment_id:08}.seg"))
    }

    fn write_segment_entry<W: Write>(
        &self,
        entry: &SegmentIndexEntry,
        segment_files: &mut HashMap<u64, File>,
        writer: &mut W,
        hasher: &mut blake3::Hasher,
    ) -> Result<()> {
        if let std::collections::hash_map::Entry::Vacant(slot) =
            segment_files.entry(entry.segment_id)
        {
            slot.insert(File::open(self.segment_path(entry.segment_id))?);
        }

        let file = segment_files
            .get_mut(&entry.segment_id)
            .ok_or_else(|| JetError::ObjectNotFound(entry.id.clone()))?;
        file.seek(SeekFrom::Start(entry.offset))?;

        if entry.compressed {
            let mut stored = vec![0_u8; entry.stored_len as usize];
            file.read_exact(&mut stored)?;
            let bytes = zstd::decode_all(stored.as_slice())?;
            writer.write_all(&bytes)?;
            hasher.update(&bytes);
            return Ok(());
        }

        copy_exact(file, entry.stored_len, writer, Some(hasher))
    }

    fn write_legacy_path<W: Write>(
        &self,
        path: &Path,
        writer: &mut W,
        hasher: Option<&mut blake3::Hasher>,
    ) -> Result<()> {
        let file = File::open(path)?;
        if path.extension().and_then(|x| x.to_str()) == Some("zst") {
            let mut decoder = zstd::stream::read::Decoder::new(file)?;
            copy_reader(&mut decoder, writer, hasher)
        } else {
            let mut file = file;
            copy_reader(&mut file, writer, hasher)
        }
    }
}

fn detect_legacy_objects(objects_root: &Path) -> Result<bool> {
    if !objects_root.exists() {
        return Ok(false);
    }

    for entry in fs::read_dir(objects_root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            return Ok(true);
        }
        if metadata.is_dir() && fs::read_dir(path)?.next().is_some() {
            return Ok(true);
        }
    }

    Ok(false)
}

impl CheckoutSession {
    pub fn write_chunks<W: Write>(
        &mut self,
        chunks: &[CheckoutChunkSource],
        writer: &mut W,
        mut hasher: Option<&mut blake3::Hasher>,
    ) -> Result<()> {
        let mut idx = 0;
        while idx < chunks.len() {
            match &chunks[idx] {
                CheckoutChunkSource::Segment {
                    segment_id,
                    offset,
                    stored_len,
                    compressed,
                    ..
                } if !compressed => {
                    let mut total_len = *stored_len;
                    let start_offset = *offset;
                    let mut next_idx = idx + 1;
                    while next_idx < chunks.len() {
                        match &chunks[next_idx] {
                            CheckoutChunkSource::Segment {
                                segment_id: next_segment_id,
                                offset: next_offset,
                                stored_len: next_len,
                                compressed: false,
                                ..
                            } if next_segment_id == segment_id
                                && *next_offset == start_offset + total_len =>
                            {
                                total_len += *next_len;
                                next_idx += 1;
                            }
                            _ => break,
                        }
                    }

                    self.copy_segment_range(
                        *segment_id,
                        start_offset,
                        total_len,
                        writer,
                        hasher.as_deref_mut(),
                    )?;
                    idx = next_idx;
                }
                CheckoutChunkSource::Segment {
                    id,
                    segment_id,
                    offset,
                    stored_len,
                    compressed: true,
                } => {
                    self.write_compressed_segment_entry(
                        id,
                        *segment_id,
                        *offset,
                        *stored_len,
                        writer,
                        hasher.as_deref_mut(),
                    )?;
                    idx += 1;
                }
                CheckoutChunkSource::Segment {
                    segment_id,
                    offset,
                    stored_len,
                    ..
                } => {
                    self.copy_segment_range(
                        *segment_id,
                        *offset,
                        *stored_len,
                        writer,
                        hasher.as_deref_mut(),
                    )?;
                    idx += 1;
                }
                CheckoutChunkSource::Legacy(path) => {
                    self.store
                        .write_legacy_path(path, writer, hasher.as_deref_mut())?;
                    idx += 1;
                }
            }
        }
        Ok(())
    }

    fn copy_segment_range<W: Write>(
        &mut self,
        segment_id: u64,
        offset: u64,
        len: u64,
        writer: &mut W,
        hasher: Option<&mut blake3::Hasher>,
    ) -> Result<()> {
        let file = self.segment_file(segment_id)?;
        file.seek(SeekFrom::Start(offset))?;
        copy_exact(file, len, writer, hasher)
    }

    fn write_compressed_segment_entry<W: Write>(
        &mut self,
        id: &str,
        segment_id: u64,
        offset: u64,
        stored_len: u64,
        writer: &mut W,
        mut hasher: Option<&mut blake3::Hasher>,
    ) -> Result<()> {
        let file = self.segment_file(segment_id)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut stored = vec![0_u8; stored_len as usize];
        file.read_exact(&mut stored)?;
        let bytes = zstd::decode_all(stored.as_slice())?;
        writer.write_all(&bytes)?;
        if let Some(hasher) = hasher.as_deref_mut() {
            hasher.update(&bytes);
        }
        let _ = id;
        Ok(())
    }

    fn segment_file(&mut self, segment_id: u64) -> Result<&mut File> {
        if let std::collections::hash_map::Entry::Vacant(slot) =
            self.segment_files.entry(segment_id)
        {
            slot.insert(File::open(self.store.segment_path(segment_id))?);
        }
        self.segment_files
            .get_mut(&segment_id)
            .ok_or_else(|| JetError::ObjectNotFound(segment_id.to_string()))
    }
}

impl ObjectStore for FsObjectStore {
    fn put_chunk_with_id(&self, id: &str, bytes: &[u8]) -> Result<StoreChunkResult> {
        if let Some(existing) = self
            .inner
            .segment_index
            .read()
            .map_err(lock_err)?
            .get(id)
            .cloned()
        {
            return Ok(StoreChunkResult {
                id: existing.id,
                was_new: false,
                raw_size: existing.raw_len,
                compressed_size: existing.stored_len,
            });
        }

        if let Some(existing) = self.legacy_object_path(id) {
            let compressed_size = existing.metadata()?.len();
            return Ok(StoreChunkResult {
                id: id.to_string(),
                was_new: false,
                raw_size: bytes.len() as u64,
                compressed_size,
            });
        }

        let stored = if self.inner.compression_enabled {
            let mut out = Vec::new();
            let mut encoder = zstd::Encoder::new(&mut out, self.inner.compression_level)?;
            encoder.write_all(bytes)?;
            encoder.finish()?;
            out
        } else {
            bytes.to_vec()
        };

        let entry = {
            let mut state = self.inner.state.lock().map_err(lock_err)?;
            if state.current_size + stored.len() as u64 > self.inner.max_segment_size_bytes {
                rotate_segment(
                    &self.inner.segments_root,
                    &self.inner.index_file_path,
                    &mut state,
                )?;
            }

            let offset = state.current_size;
            state.segment_file.write_all(&stored)?;
            if self.inner.fsync_each_object {
                state.segment_file.sync_data()?;
            }

            state.current_size += stored.len() as u64;

            let entry = SegmentIndexEntry {
                id: id.to_string(),
                segment_id: state.current_segment_id,
                offset,
                stored_len: stored.len() as u64,
                raw_len: bytes.len() as u64,
                compressed: self.inner.compression_enabled,
            };

            write_segment_index_record(&mut state.index_file, &entry)?;
            if self.inner.fsync_each_object {
                state.index_file.sync_data()?;
            }

            entry
        };

        self.inner
            .segment_index
            .write()
            .map_err(lock_err)?
            .insert(id.to_string(), entry.clone());

        Ok(StoreChunkResult {
            id: id.to_string(),
            was_new: true,
            raw_size: bytes.len() as u64,
            compressed_size: entry.stored_len,
        })
    }

    fn get_chunk(&self, id: &str) -> Result<Vec<u8>> {
        if let Some(entry) = self
            .inner
            .segment_index
            .read()
            .map_err(lock_err)?
            .get(id)
            .cloned()
        {
            let mut file = File::open(self.segment_path(entry.segment_id))?;
            file.seek(SeekFrom::Start(entry.offset))?;
            let mut stored = vec![0_u8; entry.stored_len as usize];
            file.read_exact(&mut stored)?;
            if entry.compressed {
                return Ok(zstd::decode_all(stored.as_slice())?);
            }
            return Ok(stored);
        }

        let Some(object_path) = self.legacy_object_path(id) else {
            return Err(JetError::ObjectNotFound(id.to_string()));
        };

        let encoded = fs::read(&object_path)?;
        if object_path.extension().and_then(|x| x.to_str()) == Some("zst") {
            Ok(zstd::decode_all(encoded.as_slice())?)
        } else {
            Ok(encoded)
        }
    }
}

fn load_segment_index(
    path: &Path,
    legacy_jsonl_path: &Path,
) -> Result<HashMap<String, SegmentIndexEntry>> {
    if path.exists() {
        return load_binary_segment_index(path);
    }

    if !legacy_jsonl_path.exists() {
        return Ok(HashMap::new());
    }

    let file = File::open(legacy_jsonl_path)?;
    let reader = BufReader::new(file);
    let mut out = HashMap::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: SegmentIndexEntry = serde_json::from_str(&line)?;
        out.insert(entry.id.clone(), entry);
    }
    rewrite_binary_segment_index(path, &out)?;
    Ok(out)
}

enum OrderedChunk {
    Segment(SegmentIndexEntry),
    Legacy(PathBuf),
}

fn open_segment_state(
    segments_root: &Path,
    index_file_path: &Path,
    segment_index: &HashMap<String, SegmentIndexEntry>,
) -> Result<SegmentStoreState> {
    let current_segment_id = segment_index
        .values()
        .map(|entry| entry.segment_id)
        .max()
        .unwrap_or(1);

    let segment_path = segments_root.join(format!("{current_segment_id:08}.seg"));
    let segment_file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&segment_path)?;
    let current_size = segment_file.metadata()?.len();
    let index_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(index_file_path)?;

    Ok(SegmentStoreState {
        current_segment_id,
        current_size,
        segment_file,
        index_file,
    })
}

fn rotate_segment(
    segments_root: &Path,
    index_file_path: &Path,
    state: &mut SegmentStoreState,
) -> Result<()> {
    state.current_segment_id += 1;
    let segment_path = segments_root.join(format!("{:08}.seg", state.current_segment_id));
    state.segment_file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(segment_path)?;
    state.current_size = 0;
    state.index_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(index_file_path)?;
    Ok(())
}

fn walk_dir_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if root.is_file() {
        files.push(root.to_path_buf());
        return Ok(files);
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk_dir_files(&path)?);
        } else {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

fn copy_exact<R: Read, W: Write>(
    reader: &mut R,
    mut len: u64,
    writer: &mut W,
    mut hasher: Option<&mut blake3::Hasher>,
) -> Result<()> {
    let mut buffer = [0_u8; 256 * 1024];
    while len > 0 {
        let to_read = buffer.len().min(len as usize);
        reader.read_exact(&mut buffer[..to_read])?;
        writer.write_all(&buffer[..to_read])?;
        if let Some(hasher) = hasher.as_deref_mut() {
            hasher.update(&buffer[..to_read]);
        }
        len -= to_read as u64;
    }
    Ok(())
}

fn copy_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    mut hasher: Option<&mut blake3::Hasher>,
) -> Result<()> {
    let mut buffer = [0_u8; 256 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        if let Some(hasher) = hasher.as_deref_mut() {
            hasher.update(&buffer[..read]);
        }
    }
    Ok(())
}

fn load_binary_segment_index(path: &Path) -> Result<HashMap<String, SegmentIndexEntry>> {
    let mut file = File::open(path)?;
    let mut out = HashMap::new();
    let mut record = [0_u8; SEGMENT_INDEX_RECORD_BYTES];

    loop {
        match file.read_exact(&mut record) {
            Ok(()) => {
                let entry = decode_segment_index_record(&record)?;
                out.insert(entry.id.clone(), entry);
            }
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }

    Ok(out)
}

fn rewrite_binary_segment_index(
    path: &Path,
    index: &HashMap<String, SegmentIndexEntry>,
) -> Result<()> {
    let tmp_path = path.with_extension("idx.tmp");
    let mut file = File::create(&tmp_path)?;
    let mut entries = index.values().cloned().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    for entry in entries {
        write_segment_index_record(&mut file, &entry)?;
    }
    file.sync_all()?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn write_segment_index_record(file: &mut File, entry: &SegmentIndexEntry) -> Result<()> {
    let id_bytes = entry.id.as_bytes();
    if id_bytes.len() != SEGMENT_INDEX_ID_BYTES {
        return Err(JetError::Io(std::io::Error::other(
            "segment object id must be 64 hex characters",
        )));
    }

    let mut record = [0_u8; SEGMENT_INDEX_RECORD_BYTES];
    record[..SEGMENT_INDEX_ID_BYTES].copy_from_slice(id_bytes);
    let mut cursor = SEGMENT_INDEX_ID_BYTES;
    for value in [
        entry.segment_id,
        entry.offset,
        entry.stored_len,
        entry.raw_len,
    ] {
        record[cursor..cursor + 8].copy_from_slice(&value.to_le_bytes());
        cursor += 8;
    }
    record[cursor] = u8::from(entry.compressed);
    file.write_all(&record)?;
    Ok(())
}

fn decode_segment_index_record(
    record: &[u8; SEGMENT_INDEX_RECORD_BYTES],
) -> Result<SegmentIndexEntry> {
    let id = std::str::from_utf8(&record[..SEGMENT_INDEX_ID_BYTES])
        .map_err(|err| JetError::Io(std::io::Error::other(err.to_string())))?
        .to_string();
    let mut cursor = SEGMENT_INDEX_ID_BYTES;
    let read_u64 = |cursor: &mut usize| -> u64 {
        let value = u64::from_le_bytes(record[*cursor..*cursor + 8].try_into().expect("slice"));
        *cursor += 8;
        value
    };
    Ok(SegmentIndexEntry {
        id,
        segment_id: read_u64(&mut cursor),
        offset: read_u64(&mut cursor),
        stored_len: read_u64(&mut cursor),
        raw_len: read_u64(&mut cursor),
        compressed: record[cursor] != 0,
    })
}

fn lock_err<T>(_: std::sync::PoisonError<T>) -> JetError {
    JetError::Io(std::io::Error::other("lock poisoned"))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::object_store::{FsObjectStore, ObjectStore};

    #[test]
    fn stored_chunk_can_be_read_back() {
        let dir = tempdir().expect("tempdir");
        let store = FsObjectStore::new(dir.path(), false, 1).expect("store init");

        let data = b"jet-object-store-data";
        let write = store.put_chunk(data).expect("put chunk");
        let read = store.get_chunk(&write.id).expect("read chunk");

        assert_eq!(read, data);
        assert!(write.was_new);
    }

    #[test]
    fn stats_report_segment_usage() {
        let dir = tempdir().expect("tempdir");
        let store = FsObjectStore::new(dir.path(), false, 1).expect("store init");

        store.put_chunk(b"abc").expect("put one");
        store.put_chunk(b"def").expect("put two");

        let (count, bytes) = store.storage_stats().expect("stats");
        assert_eq!(count, 2);
        assert!(bytes >= 6);
    }
}

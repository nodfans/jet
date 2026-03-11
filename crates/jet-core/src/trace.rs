pub(crate) fn add_enabled() -> bool {
    std::env::var("JET_TRACE_ADD").ok().as_deref() == Some("1")
}

pub(crate) fn hydrate_enabled() -> bool {
    std::env::var("JET_TRACE_HYDRATE").ok().as_deref() == Some("1")
}

pub(crate) fn large_add_enabled() -> bool {
    std::env::var("JET_TRACE_LARGE_ADD").ok().as_deref() == Some("1")
}

pub(crate) fn checkout_enabled() -> bool {
    std::env::var("JET_TRACE_CHECKOUT").ok().as_deref() == Some("1")
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_add(
    load_staging_ms: u128,
    load_manifest_ms: u128,
    load_chunk_cache_ms: u128,
    collect_files_ms: u128,
    process_files_ms: u128,
    save_ms: u128,
    file_count: usize,
    reused_count: usize,
    new_count: usize,
    small_files: usize,
    large_files: usize,
    reused_unchanged: usize,
    reused_same_content: usize,
    cache_hits: usize,
    reused_boundaries: usize,
    small_read_ms: u128,
    small_store_ms: u128,
    large_digest_ms: u128,
    large_chunking_ms: u128,
    large_store_ms: u128,
    total_ms: u128,
) {
    eprintln!(
        "JET_TRACE add load_staging_ms={} load_manifest_ms={} load_chunk_cache_ms={} collect_files_ms={} process_files_ms={} save_ms={} file_count={} reused_count={} new_count={} small_files={} large_files={} reused_unchanged={} reused_same_content={} cache_hits={} reused_boundaries={} small_read_ms={} small_store_ms={} large_digest_ms={} large_chunking_ms={} large_store_ms={} total_ms={}",
        load_staging_ms,
        load_manifest_ms,
        load_chunk_cache_ms,
        collect_files_ms,
        process_files_ms,
        save_ms,
        file_count,
        reused_count,
        new_count,
        small_files,
        large_files,
        reused_unchanged,
        reused_same_content,
        cache_hits,
        reused_boundaries,
        small_read_ms,
        small_store_ms,
        large_digest_ms,
        large_chunking_ms,
        large_store_ms,
        total_ms,
    );
}

pub(crate) fn emit_large_add(
    path: &str,
    size_mb: u64,
    chunk_count: usize,
    chunking_ms: u128,
    chunk_store_ms: u128,
    file_digest_ms: u128,
    total_ms: u128,
) {
    eprintln!(
        "JET_TRACE large_add path={} size_mb={} chunk_count={} chunking_ms={} chunk_store_ms={} file_digest_ms={} total_ms={}",
        path, size_mb, chunk_count, chunking_ms, chunk_store_ms, file_digest_ms, total_ms
    );
}

pub(crate) fn emit_checkout(
    read_commit_ms: u128,
    load_index_ms: u128,
    dirty_check_ms: u128,
    load_view_ms: u128,
    cleanup_ms: u128,
    save_index_ms: u128,
    save_state_ms: u128,
    total_ms: u128,
) {
    eprintln!(
        "JET_TRACE checkout read_commit_ms={} load_index_ms={} dirty_check_ms={} load_view_ms={} cleanup_ms={} save_index_ms={} save_state_ms={} total_ms={}",
        read_commit_ms,
        load_index_ms,
        dirty_check_ms,
        load_view_ms,
        cleanup_ms,
        save_index_ms,
        save_state_ms,
        total_ms,
    );
}

pub(crate) fn emit_hydrate(
    load_commit_ms: u128,
    dirty_check_ms: u128,
    filter_ms: u128,
    prepare_dirs_ms: u128,
    resolve_chunks_ms: u128,
    write_files_ms: u128,
    save_index_ms: u128,
    file_count: usize,
    total_ms: u128,
) {
    eprintln!(
        "JET_TRACE hydrate load_commit_ms={} dirty_check_ms={} filter_ms={} prepare_dirs_ms={} resolve_chunks_ms={} write_files_ms={} save_index_ms={} file_count={} total_ms={}",
        load_commit_ms,
        dirty_check_ms,
        filter_ms,
        prepare_dirs_ms,
        resolve_chunks_ms,
        write_files_ms,
        save_index_ms,
        file_count,
        total_ms,
    );
}

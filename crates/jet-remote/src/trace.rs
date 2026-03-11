pub(crate) fn enabled() -> bool {
    std::env::var("JET_TRACE_REMOTE").ok().as_deref() == Some("1")
}

pub(crate) fn emit_pull_prefetch(head: &str) {
    eprintln!("JET_TRACE_REMOTE pull prefetch head={head}");
}

pub(crate) fn emit_fetch_manifest(
    commit_id: &str,
    file_count: usize,
    default_only: bool,
    path_count: usize,
) {
    eprintln!(
        "JET_TRACE_REMOTE fetch_manifest commit={} files={} default_only={} paths={}",
        commit_id, file_count, default_only, path_count
    );
}

pub(crate) fn emit_fetch_chunks_wanted(count: usize) {
    eprintln!("JET_TRACE_REMOTE fetch_chunks wanted={count}");
}

pub(crate) fn emit_fetch_chunks_returned(count: usize) {
    eprintln!("JET_TRACE_REMOTE fetch_chunks returned={count}");
}

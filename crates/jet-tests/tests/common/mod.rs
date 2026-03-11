use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use jet_proto::proto::repository_service_server::RepositoryServiceServer;
use jet_server::{AuthConfig, JetServer};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

#[allow(dead_code)]
pub const TEST_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

pub fn write_file(root: &Path, relative: &str, contents: &[u8]) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create dir");
    }
    fs::write(path, contents).expect("write file");
}

#[allow(dead_code)]
pub async fn spawn_server(repos_root: PathBuf) -> (SocketAddr, oneshot::Sender<()>) {
    spawn_server_with_auth(repos_root, AuthConfig::default()).await
}

#[allow(dead_code)]
pub async fn spawn_server_with_auth(
    repos_root: PathBuf,
    auth: AuthConfig,
) -> (SocketAddr, oneshot::Sender<()>) {
    let server = JetServer::with_auth(&repos_root, auth).expect("server");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let stream = TcpListenerStream::new(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                RepositoryServiceServer::new(server)
                    .max_decoding_message_size(TEST_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(TEST_MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming_shutdown(stream, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve");
    });

    (addr, shutdown_tx)
}

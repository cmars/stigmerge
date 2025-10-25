use std::path::Path;

use anyhow::Result;
use stigmerge_peer::{record::StablePeersRecord, share_resolver::ShareResolver, Retry};
use tokio_util::sync::CancellationToken;
use veilnet::Connection;

pub(crate) async fn share_info<C: Connection + Clone + Send + Sync + 'static>(
    cancel: CancellationToken,
    mut conn: C,
    share_key: &str,
    path: &Path,
) -> Result<()> {
    {
        conn.require_attachment().await?;
    }
    let retry = Retry::default();
    let (share_resolver, resolver_task) =
        ShareResolver::new_task(cancel.child_token(), retry, conn.clone(), path);
    let remote_share = share_resolver.add_share(&share_key.parse()?).await?;

    #[derive(Debug)]
    #[allow(dead_code)]
    struct Info {
        key: String,
        index_digest: String,
        route_id: String,
        payload_digest: String,
        payload_length: usize,
        files: Vec<String>,
        peer_map_key: Option<String>,
    }

    let info = Info {
        key: remote_share.key.to_string(),
        index_digest: hex::encode(remote_share.index_digest),
        route_id: remote_share.route_id.to_string(),
        payload_digest: hex::encode(remote_share.index.payload().digest()),
        payload_length: remote_share.index.payload().length(),
        files: remote_share
            .index
            .files()
            .iter()
            .map(|f| f.path().to_string_lossy().to_string())
            .collect(),
        peer_map_key: remote_share
            .header
            .peer_map()
            .map(|pm| pm.key().to_string()),
    };
    println!("{:#?}", info);

    if let Some(peer_map_ref) = remote_share.header.peer_map() {
        let mut peers_record = StablePeersRecord::new_remote(&mut conn, peer_map_ref.key()).await?;
        peers_record.load_peers(&mut conn).await?;
        let peers = peers_record
            .known_peers()
            .map(|p| p.0.to_string())
            .collect::<Vec<_>>();
        println!("peers: {:#?}", peers);
    }

    cancel.cancel();
    conn.close().await?;
    resolver_task.await??;
    Ok(())
}

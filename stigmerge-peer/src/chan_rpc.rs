use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use crate::Result;

/// Common interface for RPC services that handle requests and responses over channels.
pub trait Service {
    /// The type of request messages this service handles.
    type Request;
    /// The type of response messages this service returns.
    type Response;

    /// Run the service until cancelled.
    async fn run(self, cancel: CancellationToken) -> Result<()>;

    /// Handle a request message and produce a response.
    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response>;
}


pub struct ChanClient<Request, Response> {
    pub tx: mpsc::Sender<Request>,
    pub rx: mpsc::Receiver<Response>,
}

pub struct ChanServer<Request, Response> {
    pub tx: mpsc::Sender<Response>,
    pub rx: mpsc::Receiver<Request>,
}

pub fn pipe<Request, Response>(
    capacity: usize,
) -> (ChanClient<Request, Response>, ChanServer<Request, Response>) {
    let (client_tx, client_rx) = mpsc::channel(capacity);
    let (server_tx, server_rx) = mpsc::channel(capacity);
    (
        ChanClient {
            tx: client_tx,
            rx: server_rx,
        },
        ChanServer {
            tx: server_tx,
            rx: client_rx,
        },
    )
}

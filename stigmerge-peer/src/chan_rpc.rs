use tokio::sync::mpsc;

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

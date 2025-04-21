pub use anyhow::{Error, Result};
use tokio::{
    spawn,
    sync::mpsc,
    task::{self, JoinError},
};
use tokio_util::sync::CancellationToken;

/// Common interface for RPC services that handle requests and responses over channels.
pub trait Actor {
    /// The type of request messages this service handles.
    type Request;
    /// The type of response messages this service returns.
    type Response;

    /// Run the service until cancelled.
    fn run(
        &mut self,
        cancel: CancellationToken,
        server_ch: ChanServer<Self::Request, Self::Response>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Handle a request message and produce a response.
    fn handle(
        &mut self,
        req: &Self::Request,
    ) -> impl std::future::Future<Output = Result<Self::Response>> + Send;
}

pub struct ChanClient<Request, Response> {
    tx: mpsc::Sender<Request>,
    rx: mpsc::Receiver<Response>,
}

impl<Request: Send + Sync + 'static, Response: Send + Sync + 'static>
    ChanClient<Request, Response>
{
    pub async fn send(&self, req: Request) -> Result<()> {
        self.tx.send(req).await.map_err(|e| e.into())
    }

    pub fn try_send(&self, req: Request) -> Result<()> {
        self.tx.try_send(req).map_err(|e| e.into())
    }

    pub async fn recv(&mut self) -> Option<Response> {
        self.rx.recv().await
    }

    pub async fn recv_pending(&mut self) -> usize {
        self.rx.len()
    }
}

pub struct ChanServer<Request, Response> {
    tx: mpsc::Sender<Response>,
    rx: mpsc::Receiver<Request>,
}

impl<Request, Response: Send + Sync + 'static> ChanServer<Request, Response> {
    pub async fn send(&self, req: Response) -> Result<()> {
        self.tx.send(req).await.map_err(|e| e.into())
    }

    pub async fn recv(&mut self) -> Option<Request> {
        self.rx.recv().await
    }
}

pub struct Operator<A, Req, Resp>
where
    A: Actor<Request = Req, Response = Resp> + Clone + Send + 'static,
{
    cancel: CancellationToken,
    actor: A,
    client_ch: ChanClient<Req, Resp>,
    task: task::JoinHandle<Result<()>>,
}

const ACTOR_PIPE_CAPACITY: usize = 16;

impl<
        A: Actor<Request = Req, Response = Resp> + Clone + Send + 'static,
        Req: Send + Sync + 'static,
        Resp: Send + Sync + 'static,
    > Operator<A, Req, Resp>
{
    pub async fn new(cancel: CancellationToken, actor: A) -> Self {
        let (client_ch, server_ch) = pipe(ACTOR_PIPE_CAPACITY);
        let mut task_actor = actor.clone();
        let task_cancel = cancel.child_token();
        let task = spawn(async move { task_actor.run(task_cancel, server_ch).await });
        Self {
            cancel,
            actor,
            client_ch,
            task,
        }
    }

    pub async fn respawn(&mut self) {
        let (client_ch, server_ch) = pipe(ACTOR_PIPE_CAPACITY);
        let mut task_actor = self.actor.clone();
        let task_cancel = self.cancel.child_token();
        let task = spawn(async move { task_actor.run(task_cancel, server_ch).await });
        self.client_ch = client_ch;
        self.task = task;
    }

    pub async fn send(&mut self, req: Req) -> Result<()> {
        self.client_ch.send(req).await
    }

    pub fn try_send(&mut self, req: Req) -> Result<()> {
        self.client_ch.try_send(req)
    }

    pub async fn recv(&mut self) -> Option<Resp> {
        self.client_ch.recv().await
    }

    pub async fn recv_pending(&mut self) -> usize {
        self.client_ch.recv_pending().await
    }

    pub async fn join(self) -> std::result::Result<Result<()>, JoinError> {
        self.task.await
    }
}

fn pipe<Request, Response>(
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

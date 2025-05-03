use std::sync::Arc;

use tokio::{
    select,
    sync::{broadcast, watch, Mutex, MutexGuard},
    task::{self, JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use veilid_core::{VeilidAPIError, VeilidUpdate};

use crate::{
    error::{CancelError, Permanent, Transient},
    Node, Result,
};

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

#[derive(Clone)]
pub struct ChanClient<Request, Response> {
    tx: flume::Sender<Request>,
    rx: flume::Receiver<Response>,
}

impl<Request: Send + Sync + 'static, Response: Send + Sync + 'static>
    ChanClient<Request, Response>
{
    pub async fn send(&self, req: Request) -> Result<()> {
        self.tx.send_async(req).await.map_err(|e| e.into())
    }

    pub fn try_send(&self, req: Request) -> Result<()> {
        self.tx.try_send(req).map_err(|e| e.into())
    }

    pub async fn recv(&mut self) -> Option<Response> {
        self.rx.recv_async().await.ok()
    }

    pub async fn recv_pending(&mut self) -> usize {
        self.rx.len()
    }
}

#[derive(Clone)]
pub struct ChanServer<Request, Response> {
    tx: flume::Sender<Response>,
    rx: flume::Receiver<Request>,
}

impl<Request, Response: Send + Sync + 'static> ChanServer<Request, Response> {
    pub async fn send(&self, req: Response) -> Result<()> {
        self.tx.send_async(req).await.map_err(|e| e.into())
    }

    pub async fn recv(&mut self) -> Option<Request> {
        self.rx.recv_async().await.ok()
    }
}

pub struct Operator<Req, Resp> {
    cancel: CancellationToken,
    client_ch: ChanClient<Req, Resp>,
    tasks: task::JoinSet<Result<()>>,
}

impl<Req: Clone + Send + Sync + 'static, Resp: Clone + Send + Sync + 'static> Operator<Req, Resp> {
    pub fn new<
        A: Actor<Request = Req, Response = Resp> + Clone + Send + 'static,
        R: Runner<A> + Send + 'static,
    >(
        cancel: CancellationToken,
        actor: A,
        mut runner: R,
    ) -> Self {
        let (client_ch, server_ch) = unbounded();
        let mut task_actor = actor.clone();
        let task_cancel = cancel.child_token();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { runner.run(&mut task_actor, task_cancel, server_ch).await });
        Self {
            cancel,
            client_ch,
            tasks,
        }
    }

    pub fn new_clone_pool<
        A: Actor<Request = Req, Response = Resp> + Clone + Send + 'static,
        R: Runner<A> + Clone + Send + 'static,
    >(
        cancel: CancellationToken,
        actor: A,
        runner: R,
        n: usize,
    ) -> Self {
        let (client_ch, server_ch) = unbounded();
        let mut tasks = JoinSet::new();
        for _ in 0..n {
            let mut task_runner = runner.clone();
            let mut task_actor = actor.clone();
            let task_cancel = cancel.child_token();
            let task_server_ch = server_ch.clone();
            tasks.spawn(async move {
                task_runner
                    .run(&mut task_actor, task_cancel, task_server_ch)
                    .await
            });
        }
        Self {
            cancel,
            client_ch,
            tasks,
        }
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

    pub async fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn join(self) -> std::result::Result<Result<()>, JoinError> {
        for res in self.tasks.join_all().await.into_iter() {
            if let Err(e) = res {
                return Ok(Err(e));
            }
        }
        Ok(Ok(()))
    }
}

pub trait Runner<A: Actor> {
    fn run(
        &mut self,
        actor: &mut A,
        cancel: CancellationToken,
        server_ch: ChanServer<A::Request, A::Response>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

#[derive(Clone)]
pub struct OneShot;

impl<
        A: Actor<Request: Send + Sync + 'static, Response: Send + Sync + 'static>
            + Send
            + Sync
            + 'static,
    > Runner<A> for OneShot
{
    async fn run(
        &mut self,
        actor: &mut A,
        cancel: CancellationToken,
        server_ch: ChanServer<A::Request, A::Response>,
    ) -> Result<()> {
        actor.run(cancel, server_ch).await
    }
}

#[derive(Clone)]
pub struct UntilCancelled;

impl<
        A: Actor<Request: Clone + Send + Sync + 'static, Response: Clone + Send + Sync + 'static>
            + Send
            + Sync
            + 'static,
    > Runner<A> for UntilCancelled
{
    async fn run(
        &mut self,
        actor: &mut A,
        cancel: CancellationToken,
        server_ch: ChanServer<A::Request, A::Response>,
    ) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                _ = actor.run(cancel.child_token(), server_ch.clone()) => {
                    continue;
                }
            }
        }
    }
}

pub struct WithVeilidConnection<T, N> {
    runner: T,
    node: N,
    update_rx: broadcast::Receiver<VeilidUpdate>,
    state: Arc<Mutex<ConnectionState>>,
}

pub struct ConnectionState {
    connected: watch::Sender<bool>,
}

impl ConnectionState {
    pub fn new() -> ConnectionState {
        let (tx, _) = watch::channel(false);
        ConnectionState { connected: tx }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.connected.subscribe()
    }
}

impl<T, N: Node> WithVeilidConnection<T, N> {
    pub fn new(runner: T, node: N, state: Arc<Mutex<ConnectionState>>) -> Self {
        let update_rx = node.subscribe_veilid_update();
        Self {
            runner,
            node,
            update_rx,
            state,
        }
    }

    async fn disconnected<'a>(
        &mut self,
        cancel: CancellationToken,
        state: MutexGuard<'a, ConnectionState>,
    ) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                res = self.update_rx.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::Attachment(veilid_state_attachment) => {
                            if veilid_state_attachment.public_internet_ready {
                                info!("connected: {:?}", veilid_state_attachment);
                                state.connected.send_if_modified(|val| if !*val { *val = true; true } else { false });
                                return Ok(())
                            }
                            info!("disconnected: {:?}", veilid_state_attachment);
                        }
                        VeilidUpdate::Shutdown => {
                            warn!("shutdown");
                            cancel.cancel();
                            return Err(VeilidAPIError::Shutdown.into())
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

impl<
        T: Runner<A> + Send + Sync + 'static,
        A: Actor<Request: Clone + Send + Sync + 'static, Response: Clone + Send + Sync + 'static>
            + Send
            + Sync
            + 'static,
        N: Node,
    > Runner<A> for WithVeilidConnection<T, N>
{
    async fn run(
        &mut self,
        actor: &mut A,
        cancel: CancellationToken,
        server_ch: ChanServer<<A as Actor>::Request, <A as Actor>::Response>,
    ) -> Result<()> {
        let state = self.state.clone();
        loop {
            {
                let st = state.lock().await;
                if !*st.connected.borrow() {
                    self.disconnected(cancel.clone(), st).await?;
                    continue;
                }
            }
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                res = self.runner.run(actor, cancel.child_token(), server_ch.clone()) => {
                    match res {
                        Ok(())=>return Ok(()),
                        Err(e) => {
                            error!("{:?}", e);
                            if e.is_transient() {
                                // TODO: backoff retry? circuit breaker?
                                continue;
                            }
                            if e.is_permanent() {
                                cancel.cancel();
                                return Err(e);
                            }
                            {
                                if let Ok(st) = state.try_lock() {
                                    info!("marking disconnected");
                                    st.connected.send_if_modified(|val| if *val { *val = false; true } else { false });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<T: Clone, N: Clone> Clone for WithVeilidConnection<T, N> {
    fn clone(&self) -> Self {
        Self {
            runner: self.runner.clone(),
            node: self.node.clone(),
            update_rx: self.update_rx.resubscribe(),
            state: self.state.clone(),
        }
    }
}

fn unbounded<Request, Response>() -> (ChanClient<Request, Response>, ChanServer<Request, Response>)
{
    let (client_tx, client_rx) = flume::unbounded();
    let (server_tx, server_rx) = flume::unbounded();
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

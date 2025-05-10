use std::{sync::Arc, time::Duration};

use backoff::backoff::Backoff;
use tokio::{
    select, spawn,
    sync::{broadcast, watch, Mutex, MutexGuard},
    task::{self, JoinError, JoinSet},
    time::sleep,
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
    #[tracing::instrument(skip_all)]
    pub fn new<
        A: Actor<Request = Req, Response = Resp> + Clone + Send + 'static,
        R: Runner<A> + Send + 'static,
    >(
        cancel: CancellationToken,
        actor: A,
        mut runner: R,
    ) -> Self {
        let (client_ch, server_ch) = unbounded();
        let task_actor = actor.clone();
        let task_cancel = cancel.child_token();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { runner.run(task_actor, task_cancel, server_ch).await });
        Self {
            cancel,
            client_ch,
            tasks,
        }
    }

    #[tracing::instrument(skip_all)]
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
            let task_actor = actor.clone();
            let task_cancel = cancel.child_token();
            let task_server_ch = server_ch.clone();
            tasks.spawn(async move {
                task_runner
                    .run(task_actor, task_cancel, task_server_ch)
                    .await
            });
        }
        Self {
            cancel,
            client_ch,
            tasks,
        }
    }

    #[tracing::instrument(skip_all, err)]
    pub async fn send(&mut self, req: Req) -> Result<()> {
        self.client_ch.send(req).await
    }

    #[tracing::instrument(skip_all, err)]
    pub fn try_send(&mut self, req: Req) -> Result<()> {
        self.client_ch.try_send(req)
    }

    #[tracing::instrument(skip_all)]
    pub async fn recv(&mut self) -> Option<Resp> {
        self.client_ch.recv().await
    }

    #[tracing::instrument(skip_all, ret)]
    pub async fn recv_pending(&mut self) -> usize {
        self.client_ch.recv_pending().await
    }

    #[tracing::instrument(skip_all)]
    pub async fn cancel(&self) {
        self.cancel.cancel();
    }

    #[tracing::instrument(skip_all, err)]
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
        actor: A,
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
    #[tracing::instrument(skip_all, err)]
    async fn run(
        &mut self,
        mut actor: A,
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
    #[tracing::instrument(skip_all, err)]
    async fn run(
        &mut self,
        mut actor: A,
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

pub struct WithVeilidConnection<N> {
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

impl<N: Node> WithVeilidConnection<N> {
    pub fn new(node: N, state: Arc<Mutex<ConnectionState>>) -> Self {
        let update_rx = node.subscribe_veilid_update();
        Self {
            node,
            update_rx,
            state,
        }
    }

    #[tracing::instrument(skip_all, err)]
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

const MAX_TRANSIENT_RETRIES: u8 = 5;

impl<
        A: Actor<Request: Clone + Send + Sync + 'static, Response: Clone + Send + Sync + 'static>
            + Send
            + Sync
            + 'static,
        N: Node,
    > Runner<A> for WithVeilidConnection<N>
{
    #[tracing::instrument(skip_all, err)]
    async fn run(
        &mut self,
        actor_inner: A,
        cancel: CancellationToken,
        server_ch: ChanServer<<A as Actor>::Request, <A as Actor>::Response>,
    ) -> Result<()> {
        let state = self.state.clone();
        let mut exp_backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_interval(Duration::from_secs(5))
            .build();
        let mut retries = 0;
        let actor = Arc::new(Mutex::new(actor_inner));
        loop {
            {
                let st = state.lock().await;
                if !*st.connected.borrow() {
                    self.disconnected(cancel.clone(), st).await?;
                    continue;
                }
            }

            let actor_handle = actor.clone();
            let actor_cancel = cancel.child_token();
            let actor_server_ch = server_ch.clone();
            let actor_task = spawn(async move {
                actor_handle
                    .lock()
                    .await
                    .run(actor_cancel.child_token(), actor_server_ch.clone())
                    .await
            });
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                join_res = actor_task => {
                    let res = join_res?;
                    warn!("actor run: {:?}", res);
                    match res {
                        Ok(())=>{
                            info!("actor run: ok");
                            return Ok(());
                        }
                        Err(e) => {
                            error!("{:?}", e);
                            if e.is_transient() {
                                sleep(exp_backoff.next_backoff().unwrap_or(exp_backoff.max_interval)).await;
                                retries += 1;
                                if retries < MAX_TRANSIENT_RETRIES {
                                    continue;
                                }
                                warn!("too many transient errors");
                            } else if e.is_permanent() {
                                cancel.cancel();
                                return Err(e);
                            }
                            {
                                if let Ok(st) = state.try_lock() {
                                    info!("marking disconnected");
                                    self.node.reset().await?;
                                    st.connected.send_if_modified(|val| if *val { *val = false; true } else { false });
                                    exp_backoff.reset();
                                    retries = 0;
                                }
                            }
                        }
                    }
                }
                res = self.update_rx.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::Attachment(veilid_state_attachment) => {
                            if !veilid_state_attachment.public_internet_ready {
                                if let Ok(st) = state.try_lock() {
                                    info!("marking disconnected: {:?}", veilid_state_attachment);
                                    st.connected.send_if_modified(|val| if *val { *val = false; true } else { false });
                                }
                            }
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

impl<N: Clone> Clone for WithVeilidConnection<N> {
    fn clone(&self) -> Self {
        Self {
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

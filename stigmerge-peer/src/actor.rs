use std::{sync::Arc, time::Duration};

use backoff::backoff::Backoff;
use tokio::{
    select, spawn,
    sync::{oneshot, watch, Mutex, MutexGuard},
    task::{self, JoinSet},
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
    type Request: Respondable<Response = Self::Response>;

    /// The type of response messages this service returns.
    type Response;

    /// Run the service until cancelled.
    fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Handle a request message and produce a response.
    fn handle_request(
        &mut self,
        req: Self::Request,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

pub trait Respondable {
    type Response: Send + Sync;

    fn with_response(&mut self) -> oneshot::Receiver<Self::Response>;

    fn response_tx(self) -> Option<oneshot::Sender<Self::Response>>;
}

pub struct Operator<Req> {
    cancel: CancellationToken,
    request_tx: flume::Sender<Req>,
    tasks: task::JoinSet<Result<()>>,
}

impl<Req: Respondable + Send + Sync + 'static> Operator<Req> {
    #[tracing::instrument(skip_all)]
    pub fn new<A: Actor<Request = Req> + Clone + Send + 'static, R: Runner<A> + Send + 'static>(
        cancel: CancellationToken,
        actor: A,
        mut runner: R,
    ) -> Self {
        let (request_tx, request_rx) = flume::unbounded();
        let task_actor = actor.clone();
        let task_cancel = cancel.child_token();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { runner.run(task_actor, task_cancel, request_rx).await });
        Self {
            cancel,
            request_tx,
            tasks,
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn new_clone_pool<
        A: Actor<Request = Req> + Clone + Send + 'static,
        R: Runner<A> + Clone + Send + 'static,
    >(
        cancel: CancellationToken,
        actor: A,
        runner: R,
        n: usize,
    ) -> Self {
        let (request_tx, request_rx) = flume::unbounded();
        let mut tasks = JoinSet::new();
        for _ in 0..n {
            let mut task_runner = runner.clone();
            let task_actor = actor.clone();
            let task_cancel = cancel.child_token();
            let task_request_rx = request_rx.clone();
            tasks.spawn(async move {
                task_runner
                    .run(task_actor, task_cancel, task_request_rx)
                    .await
            });
        }
        Self {
            cancel,
            request_tx,
            tasks,
        }
    }

    pub async fn call(&mut self, mut req: Req) -> Result<Req::Response> {
        let resp_rx = req.with_response();
        self.request_tx.send(req)?;
        Ok(resp_rx.await?)
    }

    pub async fn defer(
        &mut self,
        mut req: Req,
        resp_tx: flume::Sender<Req::Response>,
    ) -> Result<()> {
        let resp_rx = req.with_response();
        self.tasks.spawn(async move {
            resp_tx.send_async(resp_rx.await?).await?;
            Ok(())
        });
        self.request_tx.send(req)?;
        Ok(())
    }

    pub(super) async fn send(&mut self, req: Req) -> Result<()> {
        self.request_tx.send(req)?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn join(self) -> Result<()> {
        for res in self.tasks.join_all().await.into_iter() {
            if let Err(e) = res {
                return Err(e);
            }
        }
        Ok(())
    }
}

pub trait Runner<A: Actor> {
    fn run(
        &mut self,
        actor: A,
        cancel: CancellationToken,
        request_rx: flume::Receiver<A::Request>,
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
        mut actor: A,
        cancel: CancellationToken,
        request_rx: flume::Receiver<A::Request>,
    ) -> Result<()> {
        actor.run(cancel, request_rx).await
    }
}

#[derive(Clone)]
pub struct UntilCancelled;

impl<
        A: Actor<Request: Send + Sync + 'static, Response: Send + Sync + 'static>
            + Send
            + Sync
            + 'static,
    > Runner<A> for UntilCancelled
{
    async fn run(
        &mut self,
        mut actor: A,
        cancel: CancellationToken,
        request_rx: flume::Receiver<A::Request>,
    ) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                _ = actor.run(cancel.child_token(), request_rx.clone()) => {
                    continue;
                }
            }
        }
    }
}

pub struct WithVeilidConnection<N> {
    node: N,
    update_rx: flume::Receiver<VeilidUpdate>,
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
                res = self.update_rx.recv_async() => {
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
        A: Actor<Request: Send + Sync + 'static, Response: Send + Sync + 'static>
            + Send
            + Sync
            + 'static,
        N: Node,
    > Runner<A> for WithVeilidConnection<N>
{
    async fn run(
        &mut self,
        actor_inner: A,
        cancel: CancellationToken,
        request_rx: flume::Receiver<A::Request>,
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
            let actor_request_rx = request_rx.clone();
            let actor_task = spawn(async move {
                actor_handle
                    .lock()
                    .await
                    .run(actor_cancel.child_token(), actor_request_rx.clone())
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
                        Ok(()) => {
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
                res = self.update_rx.recv_async() => {
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
            update_rx: self.update_rx.clone(),
            state: self.state.clone(),
        }
    }
}

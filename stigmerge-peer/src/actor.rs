use std::{sync::Arc, time::Duration};

use backoff::backoff::Backoff;
use tokio::{
    select, spawn,
    sync::{broadcast, oneshot, watch, Mutex, MutexGuard},
    task::{self, JoinSet},
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use veilid_core::{VeilidAPIError, VeilidUpdate};

use crate::{
    error::{CancelError, Result, Transient},
    is_cancelled, is_unrecoverable, Error, Node,
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

pub enum ResponseChannel<Resp> {
    Drop,
    Call(Option<oneshot::Sender<Resp>>),
    Defer(Option<flume::Sender<Resp>>),
}

impl<Resp> Default for ResponseChannel<Resp> {
    fn default() -> Self {
        ResponseChannel::Drop
    }
}

impl<Resp: Send + Sync + 'static> ResponseChannel<Resp> {
    pub fn new_call() -> (Self, oneshot::Receiver<Resp>) {
        let (tx, rx) = oneshot::channel();
        (Self::Call(Some(tx)), rx)
    }

    pub async fn send(&mut self, resp: Resp) -> Result<()> {
        match self {
            Self::Drop => Ok(()),
            Self::Call(ch) => {
                if ch.is_some() {
                    ch.take()
                        .unwrap()
                        .send(resp)
                        .map_err(|_| Error::msg("failed to respond to call"))
                } else {
                    Err(Error::msg("no response or already sent"))
                }
            }
            Self::Defer(ch) => {
                if ch.is_some() {
                    ch.take()
                        .unwrap()
                        .send_async(resp)
                        .await
                        .map_err(|e| e.into())
                } else {
                    Err(Error::msg("no response or already sent"))
                }
            }
        }
    }
}

impl<Resp> From<oneshot::Sender<Resp>> for ResponseChannel<Resp> {
    fn from(value: oneshot::Sender<Resp>) -> Self {
        ResponseChannel::Call(Some(value))
    }
}

impl<Resp> From<flume::Sender<Resp>> for ResponseChannel<Resp> {
    fn from(value: flume::Sender<Resp>) -> Self {
        ResponseChannel::Defer(Some(value))
    }
}

pub trait Respondable {
    type Response: Send + Sync;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>);

    fn response_tx(self) -> ResponseChannel<Self::Response>;
}

pub struct Operator<Req> {
    cancel: CancellationToken,
    request_tx: flume::Sender<Req>,
    tasks: task::JoinSet<Result<()>>,
}

impl<Req: Respondable + Send + Sync + 'static> Operator<Req> {
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
        let (resp_ch, resp_rx) = ResponseChannel::new_call();
        req.set_response(resp_ch);
        self.request_tx.send_async(req).await?;
        Ok(resp_rx.await?)
    }

    pub async fn defer(
        &mut self,
        mut req: Req,
        resp_tx: flume::Sender<Req::Response>,
    ) -> Result<()> {
        req.set_response(resp_tx.into());
        self.request_tx.send_async(req).await?;
        Ok(())
    }

    pub(super) async fn send(&mut self, req: Req) -> Result<()> {
        self.request_tx.send_async(req).await?;
        Ok(())
    }

    pub async fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn join(self) -> Result<()> {
        self.tasks
            .join_all()
            .await
            .into_iter()
            .collect::<Result<()>>()?;
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

    pub(super) fn disconnect(&self) {
        self.connected.send_if_modified(|val| {
            if *val {
                *val = false;
                true
            } else {
                false
            }
        });
    }

    pub(super) fn connect(&self) {
        self.connected.send_if_modified(|val| {
            if !*val {
                *val = true;
                true
            } else {
                false
            }
        });
    }
}

pub type ConnectionStateHandle = Arc<Mutex<ConnectionState>>;

impl<N: Node> WithVeilidConnection<N> {
    pub fn new(node: N, state: ConnectionStateHandle) -> Self {
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
        let mut reset_interval = interval(Duration::from_secs(120));
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
                                state.connect();
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
                _ = reset_interval.tick() => {
                    warn!("resetting connection");
                    self.node.reset().await?;
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
            let mut conn_rx = {
                let st = state.lock().await;
                if !*st.connected.borrow() {
                    self.disconnected(cancel.clone(), st).await?;
                    continue;
                }
                st.subscribe()
            };

            let actor_handle = actor.clone();
            let actor_cancel = cancel.child_token();
            let actor_request_rx = request_rx.clone();
            let actor_task = spawn(async move {
                actor_handle
                    .lock()
                    .await
                    .run(actor_cancel, actor_request_rx.clone())
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
                            } else if is_unrecoverable(&e) {
                                cancel.cancel();
                                return Err(e);
                            } else if is_cancelled(&e) {
                                return Ok(());
                            }
                            {
                                if let Ok(st) = state.try_lock() {
                                    info!("marking disconnected");
                                    st.disconnect();
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
                                    st.disconnect();
                                    exp_backoff.reset();
                                    retries = 0;
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
                res = conn_rx.changed() => {
                    // TODO: need to mark these unrecoverable and in the app join, tear down on any unrecoverable error!
                    res?;
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

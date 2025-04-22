#![recursion_limit = "256"]

use std::time::Duration;

use anyhow::Error;
use chrono::Utc;
use clap::{command, Parser};
use rand::{random, rngs::OsRng, TryRngCore};
use sha2::{Digest, Sha256};
use stigmerge_peer::{new_routing_context, PeerState};
use tokio::{
    select, signal,
    sync::broadcast::{self, Receiver},
    task::JoinSet,
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;
use veilid_core::{
    DHTRecordDescriptor, DHTSchema, DHTSchemaDFLT, FourCC, KeyPair, PublicKey, RoutingContext,
    SecretKey, Timestamp, TimestampDuration, TypedKey, TypedKeyPair, ValueSubkeyRangeSet,
    VeilidAPIError, VeilidUpdate, CRYPTO_KIND_VLD0, PUBLIC_KEY_LENGTH,
};

static RENDEZVOUS_KIND: FourCC = CRYPTO_KIND_VLD0;

pub struct Rendezvous<'a> {
    routing_context: &'a RoutingContext,
}

impl<'a> Rendezvous<'a> {
    pub fn new(routing_context: &'a RoutingContext) -> Rendezvous<'a> {
        Rendezvous { routing_context }
    }

    pub async fn open_or_create(
        &self,
        schema: DHTSchema,
        digest: &[u8; PUBLIC_KEY_LENGTH],
    ) -> Result<(DHTRecordDescriptor, TypedKeyPair), Error> {
        let rendezvous_keypair = Self::get_owner_keypair(digest);
        match self
            .routing_context
            .create_dht_record(
                schema.clone(),
                Some(rendezvous_keypair.value),
                Some(rendezvous_keypair.kind),
            )
            .await
        {
            Ok(rec) => return Ok((rec, rendezvous_keypair)),
            Err(VeilidAPIError::Internal { message }) => {
                if message != "record already exists" {
                    return Err(VeilidAPIError::Internal { message }.into());
                }
            }
            Err(e) => return Err(e.into()),
        };

        // Key already exists in DHT, open it.
        let dht_key = self.get_dht_key(&rendezvous_keypair.value.key, &schema)?;
        Ok((
            self.routing_context
                .open_dht_record(dht_key, Some(rendezvous_keypair.value))
                .await?,
            rendezvous_keypair,
        ))
    }

    pub fn get_owner_keypair(digest: &[u8; 32]) -> TypedKeyPair {
        let digest_key = ed25519_dalek::SigningKey::from_bytes(&digest);
        let keypair = KeyPair::new(
            PublicKey::new(digest_key.verifying_key().as_bytes().to_owned()),
            SecretKey::new(digest_key.as_bytes().to_owned()),
        );
        TypedKeyPair::new(RENDEZVOUS_KIND, keypair)
    }

    pub fn get_dht_key(
        &self,
        owner_key: &PublicKey,
        schema: &DHTSchema,
    ) -> Result<TypedKey, Error> {
        let schema_data = schema.compile();
        let mut hash_data = Vec::<u8>::with_capacity(PUBLIC_KEY_LENGTH + 4 + schema_data.len());
        hash_data.extend_from_slice(&RENDEZVOUS_KIND.0);
        hash_data.extend_from_slice(&owner_key.bytes);
        hash_data.extend_from_slice(&schema_data);
        let hash = self
            .routing_context
            .api()
            .crypto()?
            .get(RENDEZVOUS_KIND)
            .ok_or(VeilidAPIError::unimplemented(
                "failed to resolve cryptosystem",
            ))?
            .generate_hash(&hash_data);
        Ok(TypedKey::new(RENDEZVOUS_KIND, hash))
    }
}

#[derive(Parser, Debug)]
#[command(name = "discovery-test")]
#[command(bin_name = "discovery-test")]
pub struct Cli {
    #[arg(long, env)]
    pub digest: Option<String>,

    #[arg(long, env)]
    #[clap(default_value("16"))]
    pub n_subkeys: u16,

    #[arg(long, env)]
    #[clap(default_value("3"))]
    pub n_announcer: u16,

    #[arg(long, env)]
    #[clap(default_value("10"))]
    pub n_discovery: u16,

    #[arg(long, env)]
    #[clap(default_value("3"))]
    pub n_faker: u16,

    #[arg(long, env)]
    #[clap(default_value("1"))]
    pub n_deleter: u16,
}

struct Task {
    num: u16,
    label: String,
    shutdown: CancellationToken,
    digest: [u8; 32],
    schema: DHTSchema,
    cooperate: bool,
    state_dir: tempfile::TempDir,
    metrics_tx: broadcast::Sender<Metrics>,
}

impl Task {
    fn new(
        num: u16,
        label: &str,
        shutdown: &CancellationToken,
        digest: &[u8; 32],
        schema: &DHTSchema,
        cooperate: bool,
        metrics_tx: &broadcast::Sender<Metrics>,
    ) -> Result<Task, Error> {
        Ok(Task {
            num,
            label: label.to_owned(),
            shutdown: shutdown.to_owned(),
            digest: digest.to_owned(),
            schema: schema.to_owned(),
            cooperate,
            state_dir: tempfile::tempdir()?,
            metrics_tx: metrics_tx.to_owned(),
        })
    }

    fn state_dir(&self) -> String {
        self.state_dir
            .path()
            .as_os_str()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn id(&self) -> String {
        format!("{}#{}", self.label, self.num)
    }
}

#[derive(Clone, Debug)]
struct Metrics {
    peer_restarts: u32,
    good_keys_announced: u32,
    evil_keys_announced: u32,
    good_keys_discovered: u32,
    evil_keys_discovered: u32,
}

impl Metrics {
    fn add(&mut self, other: &Metrics) {
        self.peer_restarts += other.peer_restarts;
        self.good_keys_announced += other.good_keys_announced;
        self.evil_keys_announced += other.evil_keys_announced;
        self.good_keys_discovered += other.good_keys_discovered;
        self.evil_keys_discovered += other.evil_keys_discovered;
    }

    fn with_peer_restarts(mut self, value: u32) -> Self {
        self.peer_restarts = value;
        self
    }

    fn with_good_keys_announced(mut self, value: u32) -> Self {
        self.good_keys_announced = value;
        self
    }

    fn with_evil_keys_announced(mut self, value: u32) -> Self {
        self.evil_keys_announced = value;
        self
    }

    fn with_good_keys_discovered(mut self, value: u32) -> Self {
        self.good_keys_discovered = value;
        self
    }

    fn with_evil_keys_discovered(mut self, value: u32) -> Self {
        self.evil_keys_discovered = value;
        self
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            peer_restarts: 0u32,
            good_keys_announced: 0u32,
            evil_keys_announced: 0u32,
            good_keys_discovered: 0u32,
            evil_keys_discovered: 0u32,
        }
    }
}

#[tokio::main]
async fn main() {
    run().await.expect("ok")
}

async fn run() -> Result<(), Error> {
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();

    let args = Cli::parse();

    let mut digest = [0u8; 32];
    if let Some(digest_str) = args.digest {
        hex::decode_to_slice(digest_str, &mut digest[0..32])?;
    } else {
        OsRng::default().try_fill_bytes(&mut digest)?;
    }
    println!("simulating with digest {}", hex::encode(&digest[..]));

    let schema = DHTSchema::DFLT(DHTSchemaDFLT::new(args.n_subkeys)?);

    let (metrics_tx, mut metrics_rx) = broadcast::channel(32);

    for num in 0..args.n_announcer {
        let task = Task::new(
            num,
            "announce",
            &shutdown,
            &digest,
            &schema,
            true,
            &metrics_tx,
        )?;
        tasks.spawn(async move { run_announce(task).await });
    }

    for num in 0..args.n_faker {
        let task = Task::new(
            num,
            "vandal_faker",
            &shutdown,
            &digest,
            &schema,
            false,
            &metrics_tx,
        )?;
        tasks.spawn(async move { run_announce(task).await });
    }

    for num in 0..args.n_discovery {
        let task = Task::new(
            num,
            "discovery",
            &shutdown,
            &digest,
            &schema,
            true,
            &metrics_tx,
        )?;
        tasks.spawn(async move { run_discovery(task).await });
    }

    for num in 0..args.n_deleter {
        let task = Task::new(
            num,
            "vandal_deleter",
            &shutdown,
            &digest,
            &schema,
            false,
            &metrics_tx,
        )?;
        tasks.spawn(async move { run_vandal_deleter(task).await });
    }

    let mut metric_totals = Metrics::default();

    println!("tasks have started, Ctrl-C to stop");
    let mut stats_interval = time::interval(Duration::from_secs(30));
    loop {
        select! {
            res = metrics_rx.recv() => {
                metric_totals.add(&res?);
            }
            _ = stats_interval.tick() => {
                println!("{:?}", metric_totals);
            }
            _ = signal::ctrl_c() => {
                break;
            }
            res = tasks.join_next() => {
                match res {
                    Some(Ok(Ok(()))) => {
                        println!("task completed successfully");
                    }
                    Some(Ok(Err(e))) => {
                        println!("task errored: {:?}", e);
                    }
                    Some(Err(e)) => {
                        println!("join task errored: {:?}", e);
                    }
                    None => {
                        break;
                    }
                }
                if tasks.is_empty() {
                    break;
                }
            }
        }
    }

    println!("shutting down");
    shutdown.cancel();
    tasks
        .join_all()
        .await
        .into_iter()
        .try_fold((), |_, res| match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        })?;
    println!("shutdown complete");
    println!("{:?}", metric_totals);
    Ok(())
}

async fn wait_for_attachment(
    task: &Task,
    update_rx: &mut tokio::sync::broadcast::Receiver<VeilidUpdate>,
) -> Result<(), Error> {
    let timeout = Instant::now() + Duration::from_secs(120);
    loop {
        select! {
            _ = task.shutdown.cancelled() => {
                return Ok(());
            }
            res = update_rx.recv() => {
                let update = res?;
                match update {
                    VeilidUpdate::Attachment(ref attachment) => {
                        let updated_state = PeerState::from(attachment);
                        println!(
                            "{}: node state {} attachment {:?}", task.id(), updated_state,
                            attachment);
                        if let PeerState::Connected = updated_state {
                            println!("{}: connected", task.id());
                            return Ok(());
                        }
                    }
                    VeilidUpdate::Shutdown => {
                        return Err(VeilidAPIError::Shutdown.into());
                    }
                    _ => {}
                }
            }
            _ = time::sleep_until(timeout) => {
                return Err(Error::msg(format!("{}: timeout connecting peer", task.id())));
            }
        }
    }
}

async fn connect(task: &Task) -> Result<(RoutingContext, Receiver<VeilidUpdate>), Error> {
    loop {
        let res: Result<(RoutingContext, Receiver<VeilidUpdate>), Error> = async move {
            let state_dir = task.state_dir();
            let mut state_digest = Sha256::new();
            state_digest.update(state_dir.as_bytes());
            let ns = hex::encode(state_digest.finalize());
            println!("{}: peer state dir {} ns {}", task.id(), state_dir, ns);
            let (routing_context, _, mut update_rx) =
                new_routing_context(state_dir.as_str(), Some(ns)).await?;
            wait_for_attachment(task, &mut update_rx).await?;
            Ok((routing_context, update_rx))
        }
        .await;
        match res {
            Ok(routing_context) => return Ok(routing_context),
            Err(e) => {
                println!("{}: connect attempt failed, retrying: {:?}", task.id(), e);
                task.metrics_tx
                    .send(Metrics::default().with_peer_restarts(1))?;
            }
        }
    }
}

/// Simulate a peer trying to announce content
async fn run_announce(task: Task) -> Result<(), Error> {
    loop {
        if task.shutdown.is_cancelled() {
            return Ok(());
        }
        let (routing_context, _) = connect(&task).await?;
        let res = announce(&task, &routing_context).await;
        routing_context.api().detach().await?;
        match res {
            Ok(()) => return Ok(()),
            Err(e) => {
                println!("{}: restarting on {:?}", task.id(), e);
            }
        }
    }
}

async fn announce(task: &Task, routing_context: &RoutingContext) -> Result<(), Error> {
    let rdv = Rendezvous::new(&routing_context);

    let mut interval = time::interval(Duration::from_secs(5));
    loop {
        select! {
            _ = task.shutdown.cancelled() => {
                return Ok(());
            }
            _ = interval.tick() => {
                let (dht_rec, owner) = rdv
                    .open_or_create(task.schema.clone(), &task.digest)
                    .await?;
                let subkey = random::<u32>() % task.schema.max_subkey();
                let demo_value =
                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
                routing_context
                    .set_dht_value(
                        dht_rec.key().to_owned(),
                        subkey,
                        format!("{} key: {}", if task.cooperate { "good" } else { "evil" }, demo_value).into(),
                        Some(owner.value),
                    )
                    .await?;
                println!("{}: announced subkey {} value {}", task.id(), subkey, demo_value);
                task.metrics_tx.send(
                    if task.cooperate {
                        Metrics::default().with_good_keys_announced(1)
                    } else {
                        Metrics::default().with_evil_keys_announced(1)
                    })?;
            }
        }
    }
}

/// Simulate a peer trying to discover legitimate announces
async fn run_discovery(task: Task) -> Result<(), Error> {
    loop {
        if task.shutdown.is_cancelled() {
            return Ok(());
        }
        let (routing_context, mut update_rx) = connect(&task).await?;
        let res = discovery(&task, &routing_context, &mut update_rx).await;
        routing_context.api().detach().await?;
        match res {
            Ok(()) => return Ok(()),
            Err(e) => {
                println!("{}: restarting on {}", task.id(), e);
            }
        }
    }
}

async fn discovery(
    task: &Task,
    routing_context: &RoutingContext,
    update_rx: &mut tokio::sync::broadcast::Receiver<VeilidUpdate>,
) -> Result<(), Error> {
    let rdv = Rendezvous::new(&routing_context);
    let (dht_rec, _owner_keypair) = rdv
        .open_or_create(task.schema.clone(), &task.digest)
        .await?;
    println!("{}: peer opened dht", task.id());
    loop {
        if task.shutdown.is_cancelled() {
            return Ok(());
        }
        let expiration = match routing_context
            .watch_dht_values(
                dht_rec.key().clone(),
                ValueSubkeyRangeSet::full(),
                Timestamp::now() + TimestampDuration::new(60_000_000),
                16,
            )
            .await
        {
            Ok(exp) => exp,
            Err(e) => {
                println!("{}: failed to get watch, will retry: {}", task.id(), e);
                continue;
            }
        };
        println!("{}: dht watch will expire at {}", task.id(), expiration);
        let deadline = Instant::now()
            + Duration::from_micros(expiration.saturating_sub(Timestamp::now()).as_u64());
        loop {
            select! {
                _ = time::sleep_until(deadline) => {
                    // Get another watch
                    println!("{}: this watch has ended", task.id());
                    break;
                }
                _ = task.shutdown.cancelled() => {
                    return Ok(());
                }
                res = update_rx.recv() => {
                    let update = res?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(veilid_value_change) => {
                            println!("{}: watch value change: {:?}", task.id(), veilid_value_change);
                            if let Some(value) = veilid_value_change.value {
                                let (good, evil) = ("good".as_bytes(), "evil".as_bytes());
                                let maybe_metric = if value.data().windows(4).any(|win| win == good) {
                                    Some(Metrics::default().with_good_keys_discovered(1))
                                } else if value.data().windows(4).any(|win| win == evil) {
                                    Some(Metrics::default().with_evil_keys_discovered(1))
                                } else {
                                    None
                                };
                                if let Some(metric) = maybe_metric {
                                    task.metrics_tx.send(metric)?;
                                }
                            }
                        }
                        veilid_core::VeilidUpdate::Shutdown => {
                            println!("{}: veilid api shutdown", task.id());
                            return Ok(());
                        }
                        _ => continue,
                    }
                }
            }
        }
    }
}

/// Simulate a defecting peer that tries to vandalize the content digest by
/// deleting the DHT key with the shared owner private key.
///
/// Interestingly enough, delete doesn't work if the peer has never opened it.
///
/// Soundtrack: https://www.youtube.com/watch?v=FUQhul0BJRM
async fn run_vandal_deleter(task: Task) -> Result<(), Error> {
    loop {
        if task.shutdown.is_cancelled() {
            return Ok(());
        }
        let (routing_context, _) = connect(&task).await?;
        let res = vandal_deleter(&task, &routing_context).await;
        routing_context.api().detach().await?;
        match res {
            Ok(()) => return Ok(()),
            Err(e) => {
                println!("{}: restarting on {}", task.id(), e);
            }
        }
    }
}

async fn vandal_deleter(task: &Task, routing_context: &RoutingContext) -> Result<(), Error> {
    let mut interval = time::interval(Duration::from_secs(20));
    loop {
        let rdv = Rendezvous::new(&routing_context);
        let schema = task.schema.clone();
        let digest = task.digest.clone();
        select! {
            _ = task.shutdown.cancelled() => {
                return Ok(());
            }
            _ = interval.tick() => {
                let (dht_rec, _owner_keypair) = rdv
                        .open_or_create(schema, &digest)
                        .await?;
                match routing_context.delete_dht_record(dht_rec.key().clone()).await {
                    Ok(_) => {
                        println!("{}: mwahahahaha deleted your key", task.id());
                    }
                    Err(e) => {
                        println!("{}: failed to delete key: {}", task.id(), e);
                    }
                }
            }
        }
    }
}

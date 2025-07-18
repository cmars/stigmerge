#![recursion_limit = "256"]

use indicatif::MultiProgress;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub mod app;
pub mod cli;
pub mod share;

pub use app::App;
pub use cli::Cli;

pub fn initialize_stdout_logging() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_ansi(false)
                .with_writer(std::io::stdout),
        )
        .with(env_filter())
        .init();
}

pub fn initialize_ui_logging(multi_progress: MultiProgress) {
    let writer = ChannelWriter::new(multi_progress);
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(true)
                .with_level(true)
                .with_target(true)
                .with_writer(move || writer.clone()),
        )
        .with(env_filter())
        .init();
}

fn env_filter() -> EnvFilter {
    if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::builder().from_env_lossy()
    } else {
        "stigmerge=debug,stigmerge_peer=debug".parse().unwrap()
    }
}

#[derive(Clone)]
struct ChannelWriter {
    multi_progress: MultiProgress,
}

impl ChannelWriter {
    fn new(multi_progress: MultiProgress) -> Self {
        ChannelWriter { multi_progress }
    }
}

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(msg) = std::str::from_utf8(buf) {
            let _ = self.multi_progress.println(msg);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

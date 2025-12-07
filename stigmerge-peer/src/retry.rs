use std::sync::Arc;

use backoff::{backoff::Backoff, ExponentialBackoff};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Retry(pub Arc<Mutex<dyn Backoff + Send + Sync>>);

impl Default for Retry {
    fn default() -> Retry {
        Self(Arc::new(Mutex::new(ExponentialBackoff::default())))
    }
}

#[macro_export]
macro_rules! backoff_retry {
    ($cancel:expr, $retry:expr, $attempt:block, $exhausted:block) => {
        {
            let mut _final_result = Ok(());
            let mut _attempt = 0u32;
            loop {
                let _result = async {
                    $attempt
                    Ok::<(), backoff::Error<$crate::Error>>(())
                }.await;
                match _result {
                    Ok(()) => {
                        if _attempt > 0 {
                            tracing::info!(attempt = _attempt, "retry successful");
                        }
                        _final_result = Ok(());
                        break;
                    }
                    Err(backoff::Error::Permanent(err)) => {
                        _final_result = Err(err);
                        break;
                    }
                    Err(backoff::Error::Transient { err, retry_after }) => {
                        let mut _retry = $retry.0.lock().await;
                        match retry_after.or(_retry.next_backoff()) {
                            Some(delay) => {
                                _attempt += 1;
                                tokio::select! {
                                    _ = $cancel.cancelled() => {
                                        _final_result = Err($crate::CancelError.into());
                                        break;
                                    }
                                    _ = tokio::time::sleep(delay) => {}
                                };
                                tracing::warn!(?delay, attempt = _attempt, "retrying");
                            }
                            None => {
                                $exhausted
                                _final_result = Err(err);
                                _retry.reset();
                                break;
                            }
                        }
                    }
                }
            }
            _final_result
        }
    };
    ($cancel:expr, $retry:expr, $attempt:block) => {
        backoff_retry!($cancel, $retry, $attempt, {})
    }
}

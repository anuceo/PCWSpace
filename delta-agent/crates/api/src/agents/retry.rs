use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;

use crate::agents::errors::AgentError;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_millis(2_500),
        }
    }
}

impl RetryPolicy {
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let factor = 2u64.saturating_pow(attempt.min(16));
        let scaled = self
            .base_delay
            .as_millis()
            .saturating_mul(u128::from(factor));
        let bounded = scaled.min(self.max_delay.as_millis());
        Duration::from_millis(bounded as u64)
    }
}

pub async fn execute_with_retry<T, Op, Fut>(
    policy: &RetryPolicy,
    mut operation: Op,
) -> Result<T, AgentError>
where
    Op: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, AgentError>>,
{
    let mut attempt = 0u32;
    loop {
        match operation(attempt).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                let should_retry = attempt < policy.max_retries && err.is_retryable();
                if !should_retry {
                    return Err(err);
                }
                let delay = policy.delay_for_attempt(attempt);
                sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

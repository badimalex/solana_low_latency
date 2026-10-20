use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Retryable,
    NonRetryable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionError {
    pub class: RetryClass,
    pub message: String,
}

impl SubmissionError {
    pub fn is_retryable(&self) -> bool {
        match self.class {
            RetryClass::Retryable => true,
            RetryClass::NonRetryable => false,
        }
    }
}

#[test]
fn retryable_submission_error_is_retryable() {
    let err = SubmissionError {
        class: RetryClass::Retryable,
        message: "temporary network error".to_string(),
    };

    assert!(err.is_retryable());
}

#[test]
fn non_retryable_submission_error_is_not_retryable() {
    let err = SubmissionError {
        class: RetryClass::NonRetryable,
        message: "validation failed".to_string(),
    };

    assert!(!err.is_retryable());
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryPolicyError {
    ZeroMaxAttempts,
    InitialDelayExceedsMaxDelay,
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: usize,
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(
        max_attempts: usize,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, RetryPolicyError> {
        if max_attempts == 0 {
            return Err(RetryPolicyError::ZeroMaxAttempts);
        }

        if initial_delay > max_delay {
            return Err(RetryPolicyError::InitialDelayExceedsMaxDelay);
        }

        Ok(Self {
            max_attempts,
            initial_delay,
            max_delay,
        })
    }

    pub fn can_retry(&self, current_attempt: usize) -> bool {
        current_attempt < self.max_attempts
    }

    pub fn delay_for(&self, current_attempt: usize) -> Duration {
        let mut res = self.initial_delay;

        for _ in 1..current_attempt {
            if res * 2 > self.max_delay {
                return self.max_delay;
            }

            res *= 2;
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_retry_before_limit() {
        let policy =
            RetryPolicy::new(3, Duration::from_millis(100), Duration::from_millis(500)).unwrap();

        assert!(policy.can_retry(2));
    }

    #[test]
    fn cannot_retry_at_limit() {
        let policy =
            RetryPolicy::new(3, Duration::from_millis(100), Duration::from_millis(500)).unwrap();

        assert!(!policy.can_retry(3));
    }

    #[test]
    fn delay_grows_exponentially() {
        let policy =
            RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(1000)).unwrap();

        assert_eq!(policy.delay_for(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for(3), Duration::from_millis(400));
    }

    #[test]
    fn delay_is_capped() {
        let policy =
            RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(250)).unwrap();

        assert_eq!(policy.delay_for(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for(3), Duration::from_millis(250));
        assert_eq!(policy.delay_for(4), Duration::from_millis(250));
    }

    #[test]
    fn rejects_zero_max_attempts() {
        let result = RetryPolicy::new(0, Duration::from_millis(100), Duration::from_millis(500));

        assert_eq!(result.unwrap_err(), RetryPolicyError::ZeroMaxAttempts);
    }

    #[test]
    fn rejects_initial_delay_above_max_delay() {
        let result = RetryPolicy::new(3, Duration::from_millis(500), Duration::from_millis(250));

        assert_eq!(
            result.unwrap_err(),
            RetryPolicyError::InitialDelayExceedsMaxDelay
        );
    }
}

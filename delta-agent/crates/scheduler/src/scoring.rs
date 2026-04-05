pub fn dynamic_score(base_priority: i32, lag_ms: u64, attempts: u32) -> i64 {
    let lag_bonus = (lag_ms / 100) as i64;
    let retry_penalty = (attempts as i64) * 25;
    (base_priority as i64 * 100) + lag_bonus - retry_penalty
}

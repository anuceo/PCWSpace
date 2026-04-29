use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityScore {
    pub clarity: f32,
    pub completeness: f32,
    pub actionability: f32,
    pub overall: f32,
}

impl QualityScore {
    pub fn compute(text: &str) -> Self {
        let clarity = score_clarity(text);
        let completeness = score_completeness(text);
        let actionability = score_actionability(text);
        let overall = (clarity + completeness + actionability) / 3.0;
        Self { clarity, completeness, actionability, overall }
    }
}

fn score_clarity(text: &str) -> f32 {
    let words = text.split_whitespace().count();
    let sentences = text.split(['.', '!', '?']).filter(|s| !s.trim().is_empty()).count();
    if sentences == 0 { return 0.5; }
    let avg_words = words as f32 / sentences as f32;
    // Ideal sentence length ~15-20 words → score 1.0, penalise divergence
    let score = 1.0 - ((avg_words - 17.0).abs() / 40.0).min(1.0);
    score.max(0.1)
}

fn score_completeness(text: &str) -> f32 {
    let len = text.len();
    // Short responses < 50 chars: penalised. Very long > 2000: full marks.
    match len {
        0..=49   => 0.2,
        50..=199 => 0.5,
        200..=999 => 0.8,
        _ => 1.0,
    }
}

fn score_actionability(text: &str) -> f32 {
    let action_words = [
        "implement", "create", "build", "add", "fix", "update", "run",
        "deploy", "configure", "install", "use", "call", "return", "check",
    ];
    let lower = text.to_lowercase();
    let count = action_words.iter().filter(|w| lower.contains(*w)).count();
    (count as f32 * 0.15).min(1.0).max(0.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_reasonable_range() {
        let score = QualityScore::compute("Implement the login feature using JWT tokens. Run tests after.");
        assert!(score.overall > 0.0 && score.overall <= 1.0);
    }
}

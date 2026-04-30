use pcw_core::models::AgentType;
use crate::scoring::QualityScore;
use serde::{Deserialize, Serialize};

/// Quality overall score below this threshold falls back to Claude regardless
/// of the task category — vague/short prompts benefit from Claude's stronger
/// natural-language handling over a specialist model.
const LOW_QUALITY_THRESHOLD: f32 = 0.35;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAnalysis {
    pub suggested_agent: AgentType,
    pub task_category: TaskCategory,
    pub quality: QualityScore,
    pub requires_tools: bool,
}

impl TaskAnalysis {
    /// The agent that should actually handle this task.
    ///
    /// If quality is below `LOW_QUALITY_THRESHOLD` we override to Claude
    /// regardless of category — ambiguous prompts need general reasoning
    /// rather than a specialist model.
    pub fn effective_agent(&self) -> AgentType {
        if self.quality.overall < LOW_QUALITY_THRESHOLD {
            AgentType::Claude
        } else {
            self.suggested_agent.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskCategory {
    CodeGeneration,
    Debugging,
    Documentation,
    Research,
    Planning,
    DataAnalysis,
    General,
}

pub fn analyze_task(prompt: &str) -> TaskAnalysis {
    let lower = prompt.to_lowercase();
    let quality = QualityScore::compute(prompt);

    let (category, suggested_agent) = categorize(&lower);
    let requires_tools = lower.contains("search")
        || lower.contains("fetch")
        || lower.contains("read file")
        || lower.contains("execute");

    TaskAnalysis {
        suggested_agent,
        task_category: category,
        quality,
        requires_tools,
    }
}

fn categorize(lower: &str) -> (TaskCategory, AgentType) {
    if lower.contains("write code")
        || lower.contains("implement")
        || lower.contains("function")
        || lower.contains("class")
    {
        (TaskCategory::CodeGeneration, AgentType::DeepSeek)
    } else if lower.contains("debug")
        || lower.contains("fix bug")
        || lower.contains("error")
        || lower.contains("exception")
    {
        (TaskCategory::Debugging, AgentType::DeepSeek)
    } else if lower.contains("analyse")
        || lower.contains("analyze")
        || lower.contains("dataset")
        || lower.contains("statistics")
    {
        (TaskCategory::DataAnalysis, AgentType::DeepSeek)
    } else if lower.contains("document")
        || lower.contains("explain")
        || lower.contains("describe")
    {
        (TaskCategory::Documentation, AgentType::Claude)
    } else if lower.contains("research")
        || lower.contains("find out")
        || lower.contains("what is")
    {
        (TaskCategory::Research, AgentType::Claude)
    } else if lower.contains("plan")
        || lower.contains("roadmap")
        || lower.contains("strategy")
    {
        (TaskCategory::Planning, AgentType::Claude)
    } else {
        (TaskCategory::General, AgentType::Claude)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pcw_core::models::AgentType;

    #[test]
    fn low_quality_overrides_to_claude() {
        // "implement" triggers DeepSeek by category, but the prompt is so short
        // that quality.overall falls below the threshold — effective_agent must be Claude.
        let analysis = analyze_task("implement");
        assert_eq!(analysis.suggested_agent, AgentType::DeepSeek);
        assert!(analysis.quality.overall < LOW_QUALITY_THRESHOLD,
            "expected low quality, got {}", analysis.quality.overall);
        assert_eq!(analysis.effective_agent(), AgentType::Claude);
    }

    #[test]
    fn high_quality_code_task_uses_deepseek() {
        let prompt = "Implement a binary search function in Rust. \
                      It should take a sorted slice and a target value. \
                      Return the index if found, or None otherwise. \
                      Include error handling and unit tests.";
        let analysis = analyze_task(prompt);
        assert_eq!(analysis.suggested_agent, AgentType::DeepSeek);
        assert!(analysis.quality.overall >= LOW_QUALITY_THRESHOLD,
            "expected quality above threshold, got {}", analysis.quality.overall);
        assert_eq!(analysis.effective_agent(), AgentType::DeepSeek);
    }

    #[test]
    fn high_quality_general_task_uses_claude() {
        let prompt = "Research the history of event sourcing in distributed systems. \
                      Summarize the key papers and explain how it differs from CQRS.";
        let analysis = analyze_task(prompt);
        assert_eq!(analysis.effective_agent(), AgentType::Claude);
    }

    #[test]
    fn effective_agent_same_as_suggested_when_quality_ok() {
        let analysis = analyze_task(
            "Debug the authentication error that occurs when the JWT token expires \
             during a long-running API call."
        );
        assert_eq!(analysis.suggested_agent, AgentType::DeepSeek);
        assert_eq!(analysis.effective_agent(), analysis.suggested_agent);
    }
}

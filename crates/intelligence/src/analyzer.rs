use pcw_core::models::AgentType;
use crate::scoring::QualityScore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAnalysis {
    pub suggested_agent: AgentType,
    pub task_category: TaskCategory,
    pub quality: QualityScore,
    pub requires_tools: bool,
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

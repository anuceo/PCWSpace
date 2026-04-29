use pcw_core::models::{AgentType, WorkflowDefinition, WorkflowStep};

pub fn client_outreach_workflow() -> WorkflowDefinition {
    WorkflowDefinition::new_linear(
        "client_outreach",
        vec![
            WorkflowStep {
                name: "research_prospect".into(),
                handler: "research".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Research the prospect company. Summarize key info relevant to our offering.".into()),
                timeout_secs: 120,
                max_retries: 2,
            },
            WorkflowStep {
                name: "draft_outreach".into(),
                handler: "draft_email".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Draft a personalized outreach email based on the research.".into()),
                timeout_secs: 120,
                max_retries: 2,
            },
            WorkflowStep {
                name: "review_outreach".into(),
                handler: "review".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Review and improve the outreach email for clarity and impact.".into()),
                timeout_secs: 60,
                max_retries: 1,
            },
            WorkflowStep {
                name: "finalize".into(),
                handler: "finalize".into(),
                agent_type: None,
                prompt: None,
                timeout_secs: 30,
                max_retries: 0,
            },
        ],
    )
}

pub fn content_creation_workflow() -> WorkflowDefinition {
    WorkflowDefinition::new_linear(
        "content_creation",
        vec![
            WorkflowStep {
                name: "research_topic".into(),
                handler: "research".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Research the content topic thoroughly. Gather key facts, angles, and insights.".into()),
                timeout_secs: 120,
                max_retries: 2,
            },
            WorkflowStep {
                name: "create_outline".into(),
                handler: "outline".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Create a detailed content outline with sections and key points.".into()),
                timeout_secs: 90,
                max_retries: 2,
            },
            WorkflowStep {
                name: "write_draft".into(),
                handler: "write".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Write the full content draft following the outline.".into()),
                timeout_secs: 300,
                max_retries: 2,
            },
            WorkflowStep {
                name: "edit_and_polish".into(),
                handler: "edit".into(),
                agent_type: Some(AgentType::Claude),
                prompt: Some("Edit the draft for grammar, flow, and quality. Polish the final output.".into()),
                timeout_secs: 120,
                max_retries: 1,
            },
            WorkflowStep {
                name: "save_artifact".into(),
                handler: "save".into(),
                agent_type: None,
                prompt: None,
                timeout_secs: 30,
                max_retries: 0,
            },
        ],
    )
}

pub fn get_definition(name: &str) -> Option<WorkflowDefinition> {
    match name {
        "client_outreach" => Some(client_outreach_workflow()),
        "content_creation" => Some(content_creation_workflow()),
        _ => None,
    }
}

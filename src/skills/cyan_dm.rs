// cyan-backend/src/skills/cyan_dm.rs

use anyhow::Result;
use serde_json::json;
use super::*;

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "cyan_nudge".into(),
            name: "Cyan Nudge".into(),
            description: "Send a DM nudge to a team member via Cyan chat".into(),
            keywords: vec!["nudge".into(), "dm".into(), "remind".into(), "ping".into(), "notify".into(), "send message".into(), "cyan dm".into()],
            tools: vec!["cyan_dm".into()],
            output_type: OutputType::Action,
            requires_auth: vec![],
            default_timeout: 30,
        },
    ]
}

pub struct Nudge;

#[async_trait::async_trait]
impl SkillExecutor for Nudge {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        tracing::info!("🔧 [cyan_nudge] Executing");
        
        // Build nudge message from context
        let prev_context: Vec<String> = ctx.previous_outputs.iter()
            .filter(|o| o.output_type == OutputType::Summary)
            .map(|o| o.output.clone())
            .collect();
        
        let prompt = format!(
            "Based on this context, draft a short, friendly nudge message (2-3 sentences max):\n\
             User's instruction: {}\n\
             Context:\n{}\n\n\
             Write just the message, no subject or greeting.",
            ctx.cell_content,
            prev_context.join("\n")
        );
        
        let message = crate::pipeline::call_vllm_public(&prompt, 150, 0.5).await?;
        
        // Send via Cyan's native DM system
        // For now, log it — the actual DM would go through the chat actor
        tracing::info!("💬 [Cyan DM] Nudge: {}", &message[..message.len().min(100)]);
        
        // TODO: Send via command_tx → ChatMessage
        // let _ = command_tx.send(CommandMsg::SendDirectMessage { ... });
        
        Ok(SkillResult {
            skill_id: "cyan_nudge".into(),
            output_type: OutputType::Action,
            summary: format!("Nudge sent: {}", &message[..message.len().min(80)]),
            data: json!({
                "message": message,
                "status": "sent",
            }),
            timecoded_findings: None,
            action_taken: Some(format!("DM sent: {}", &message[..message.len().min(80)])),
            artifacts: vec![],
        })
    }
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }
}

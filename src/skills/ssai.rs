// cyan-backend/src/skills/ssai.rs
//
// Server-Side Ad Insertion skill
// Detects optimal ad break points using scene analysis + LLM reasoning
// Outputs timecoded notes marking insertion points

use anyhow::{anyhow, Result};
use serde_json::json;
use crate::skills::{SkillExecutor, SkillDef, SkillContext, SkillResult, OutputType, Finding};

// ============================================================================
// SSAI Skill Definitions
// ============================================================================

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "ssai_break_detection".into(),
            name: "Ad Break Detection".into(),
            description: "Detect optimal ad insertion points using scene analysis and AI reasoning".into(),
            keywords: vec![
                "ssai".into(), "ad".into(), "insertion".into(), "break".into(),
                "advertising".into(), "mid-roll".into(), "pre-roll".into(),
                "commercial".into(), "scte".into(), "splice".into(),
                "monetization".into(), "ad break".into(),
            ],
            tools: vec!["ffprobe".into(), "ffmpeg".into(), "vllm".into()],
            output_type: OutputType::TimecodedNotes, requires_auth: vec![], default_timeout: 300,
        },
        SkillDef {
            id: "ssai_compliance".into(),
            name: "Ad Compliance Check".into(),
            description: "Verify ad insertion plan meets territory broadcast regulations (TRAI, CBFC, IMDA)".into(),
            keywords: vec![
                "ad compliance".into(), "trai".into(), "ad frequency".into(),
                "advertising regulation".into(), "ad limit".into(),
            ],
            tools: vec!["vllm".into()],
            output_type: OutputType::Summary, requires_auth: vec![], default_timeout: 300,
        },
    ]
}

// ============================================================================
// Ad Break Detection
// ============================================================================

pub struct AdBreakDetection;

#[async_trait::async_trait]
impl SkillExecutor for AdBreakDetection {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
            tracing::info!("📺 [SSAI] Detecting ad break points");

            // Step 1: Get video URI
            let video_uri = ctx.video_uri.as_deref()
                .or_else(|| {
                    // Try to find video URL in cell content or previous outputs
                    for output in &ctx.previous_outputs {
                        for word in output.output.split_whitespace() {
                            if (word.starts_with("http") || word.starts_with("s3://"))
                                && (word.contains(".mp4") || word.contains(".mov") || word.contains(".mkv"))
                            {
                                return Some(word);
                            }
                        }
                    }
                    None
                })
                .ok_or_else(|| anyhow!("No video URI found for SSAI analysis"))?;

            // Step 2: Run ffprobe for duration and scene detection
            let probe_output = tokio::process::Command::new("ffprobe")
                .args([
                    "-v", "quiet",
                    "-print_format", "json",
                    "-show_format",
                    "-show_streams",
                    video_uri,
                ])
                .output()
                .await
                .map_err(|e| anyhow!("ffprobe failed: {}", e))?;

            let probe_json = String::from_utf8_lossy(&probe_output.stdout);
            let duration = extract_duration(&probe_json);

            tracing::info!("📺 [SSAI] Video duration: {:.1}s", duration);

            // Step 3: Run scene detection via ffmpeg
            let scene_output = tokio::process::Command::new("ffprobe")
                .args([
                    "-v", "quiet",
                    "-show_entries", "frame=pts_time,pkt_pts_time",
                    "-of", "csv=p=0",
                    "-f", "lavfi",
                    &format!("movie={},select=gt(scene\\,0.35)", video_uri),
                ])
                .output()
                .await;

            let scene_changes: Vec<f64> = if let Ok(output) = scene_output {
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .filter_map(|line| line.split(',').next()?.parse::<f64>().ok())
                    .collect()
            } else {
                // Fallback: generate evenly-spaced candidate points
                tracing::warn!("📺 [SSAI] Scene detection failed, using interval-based detection");
                generate_interval_points(duration)
            };

            tracing::info!("📺 [SSAI] Found {} scene change candidates", scene_changes.len());

            // Step 4: Get transcript context from previous steps (if available)
            let transcript_context = ctx.previous_outputs.iter()
                .find(|o| o.step_id.contains("transcript") || o.step_id.contains("transcri"))
                .map(|o| o.output.clone())
                .unwrap_or_else(|| "No transcript available".to_string());

            // Step 5: Send to LLM for intelligent break point selection
            let scene_list = scene_changes.iter()
                .map(|t| format!("  - {}", format_timecode(*t)))
                .collect::<Vec<_>>()
                .join("\n");

            let prompt = format!(
                r#"You are an ad insertion specialist for broadcast television. 

VIDEO DURATION: {:.0} seconds ({})
SCENE CHANGES DETECTED:
{}

TRANSCRIPT CONTEXT (if available):
{}

BROADCAST REGULATIONS:
- India (TRAI): Maximum 12 minutes of ads per hour. Minimum 15 minutes between ad breaks.
- General: No mid-sentence breaks. No breaks during critical dramatic moments.
- Pre-roll: 0-15 seconds before content
- Mid-roll: Natural pause points (scene transitions, dialogue gaps)
- Post-roll: After content ends

TASK: Select 3-5 optimal ad insertion points from the scene changes above. For each point:
1. Timecode (in seconds)
2. Type: pre-roll, mid-roll, or post-roll
3. Recommended ad duration (15s, 30s, or 60s)
4. Reason why this is a good break point
5. Content category suggestion (what type of ad fits the surrounding content)

Respond in JSON array format:
[
  {{
    "timecode_seconds": 0.0,
    "type": "pre-roll",
    "duration": "15s",
    "reason": "Before content begins",
    "content_match": "general"
  }}
]"#,
                duration,
                format_timecode(duration),
                if scene_list.is_empty() { "  No scene changes detected — use interval-based points".to_string() } else { scene_list },
                &transcript_context[..transcript_context.len().min(500)],
            );

            let ai_response = crate::pipeline::call_vllm_public(&prompt, 800, 0.3).await?;

            // Step 6: Parse LLM response into timecoded findings
            let findings = parse_ssai_response(&ai_response, duration);

            let summary = format!(
                "SSAI Analysis: {} ad break points identified in {} video.\n\
                 Total ad time: {}s\n\
                 TRAI compliant: {}\n\n{}",
                findings.len(),
                format_timecode(duration),
                findings.iter()
                    .map(|f| f.content.split(" — ").next().unwrap_or("30s")
                        .chars().filter(|c| c.is_ascii_digit()).collect::<String>()
                        .parse::<u32>().unwrap_or(30))
                    .sum::<u32>(),
                check_trai_compliance(&findings, duration),
                findings.iter()
                    .map(|f| format!("  {} — {}", format_timecode(f.timecode_seconds), f.content))
                    .collect::<Vec<_>>()
                    .join("\n"),
            );

            Ok(SkillResult {
                skill_id: "ssai_break_detection".into(),
                summary,
                output_type: OutputType::TimecodedNotes,
                data: json!({"break_points": findings.len(), "duration": duration}),
                timecoded_findings: Some(findings), action_taken: None, artifacts: vec![],
            })
    }
}

// ============================================================================
// Ad Compliance Check
// ============================================================================

pub struct AdComplianceCheck;

#[async_trait::async_trait]
impl SkillExecutor for AdComplianceCheck {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[1].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
            tracing::info!("📺 [SSAI] Running ad compliance check");

            // Get SSAI results from previous step
            let ssai_output = ctx.previous_outputs.iter()
                .find(|o| o.step_id.contains("ssai") || o.step_id.contains("ad_break"))
                .map(|o| o.output.clone())
                .unwrap_or_else(|| ctx.cell_content.clone());

            let prompt = format!(
                r#"You are a broadcast compliance officer. Review the following ad insertion plan and check against regulations for each territory.

AD INSERTION PLAN:
{}

CHECK AGAINST THESE REGULATIONS:

INDIA (TRAI - Telecom Regulatory Authority of India):
- Max 12 minutes of ads per clock hour
- Min 15 minutes gap between ad breaks
- No ads during national anthem
- Tobacco/alcohol ads prohibited during children's programming
- News channels: no product placement within news segments

SINGAPORE (IMDA):
- Max 14 minutes of ads per hour for free-to-air
- No ads for gambling, tobacco
- Children's programming: no food ads for unhealthy products

UAE (NMC):
- Max 15 minutes of ads per hour
- No ads during prayer times
- Alcohol/pork product ads prohibited

For each territory, provide:
1. COMPLIANT / WARNING / VIOLATION
2. Specific issue (if any)
3. Recommended fix

Respond as a compliance matrix."#,
                &ssai_output[..ssai_output.len().min(1000)],
            );

            let ai_response = crate::pipeline::call_vllm_public(&prompt, 600, 0.2).await?;

            Ok(SkillResult {
                skill_id: "ssai_compliance".into(),
                summary: ai_response,
                output_type: OutputType::Summary,
                data: json!({"territories_checked": ["India", "Singapore", "UAE"]}),
                timecoded_findings: None, action_taken: None, artifacts: vec![],
            })
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_duration(probe_json: &str) -> f64 {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(probe_json)
        && let Some(dur) = v["format"]["duration"].as_str() {
            return dur.parse::<f64>().unwrap_or(600.0);
        }
    600.0 // default 10 min
}

fn generate_interval_points(duration: f64) -> Vec<f64> {
    let interval = if duration > 3600.0 { 900.0 }      // 15 min for long content
        else if duration > 1800.0 { 600.0 }              // 10 min for medium
        else if duration > 600.0 { 300.0 }                // 5 min for short
        else { 180.0 };                                    // 3 min for very short

    let mut points = Vec::new();
    let mut t = interval;
    while t < duration - 30.0 {
        points.push(t);
        t += interval;
    }
    points
}

fn format_timecode(seconds: f64) -> String {
    let total = seconds as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

fn parse_ssai_response(response: &str, duration: f64) -> Vec<Finding> {
    // Try to extract JSON array from response
    let json_str = if let Some(start) = response.find('[') {
        if let Some(end) = response.rfind(']') {
            &response[start..=end]
        } else {
            response
        }
    } else {
        response
    };

    if let Ok(breaks) = serde_json::from_str::<Vec<serde_json::Value>>(json_str) {
        breaks.iter().map(|b| {
            let tc = b["timecode_seconds"].as_f64().unwrap_or(0.0);
            let ad_type = b["type"].as_str().unwrap_or("mid-roll");
            let ad_duration = b["duration"].as_str().unwrap_or("30s");
            let reason = b["reason"].as_str().unwrap_or("Scene transition");
            let content = b["content_match"].as_str().unwrap_or("general");

            Finding {
                timecode_seconds: tc,
                content: format!("📺 {} — {} ad break ({})", ad_type, ad_duration, reason),
                finding_type: "ad_break".to_string(), source: "ssai".to_string(),
                severity: "info".to_string(),
                suggested_action: Some(format!("Insert {} {} ad. Content category: {}", ad_duration, ad_type, content)),
            }
        }).collect()
    } else {
        // Fallback: generate default break points
        let points = generate_interval_points(duration);
        points.iter().enumerate().map(|(i, t)| {
            Finding {
                timecode_seconds: *t,
                content: format!("📺 mid-roll — 30s ad break (interval-based point #{})", i + 1),
                finding_type: "ad_break".to_string(), source: "ssai".to_string(),
                severity: "info".to_string(),
                suggested_action: Some("Insert 30s mid-roll ad at natural interval point".to_string()),
            }
        }).collect()
    }
}

fn check_trai_compliance(findings: &[Finding], duration: f64) -> &'static str {
    // Check TRAI: max 12 min ads per hour, min 15 min between breaks
    let total_ad_seconds: f64 = findings.len() as f64 * 30.0; // assume 30s average
    let hours = duration / 3600.0;
    let max_allowed = hours * 720.0; // 12 min * 60s = 720s per hour

    if total_ad_seconds > max_allowed {
        "⚠️ WARNING — exceeds TRAI 12 min/hour limit"
    } else {
        // Check minimum gap
        let mut prev_tc = 0.0;
        for f in findings {
            if f.timecode_seconds - prev_tc < 900.0 && prev_tc > 0.0 {
                return "⚠️ WARNING — breaks less than 15 min apart";
            }
            prev_tc = f.timecode_seconds;
        }
        "✅ Compliant"
    }
}

// cyan-backend/src/skills/media.rs
//
// Media pipeline skills for video post-production.
// Uses real tools (ffprobe, ffmpeg) + AI analysis via vLLM.
// QC analysis generates timecoded findings that become video notes.

use anyhow::{anyhow, Result};
use serde_json::json;
use super::*;

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "ffprobe_metadata".into(),
            name: "Video Metadata".into(),
            description: "Extract video metadata: codec, resolution, duration, audio tracks".into(),
            keywords: vec!["ingest".into(), "metadata".into(), "video info".into(), "ffprobe".into(), "format".into()],
            tools: vec!["ffprobe".into()],
            output_type: OutputType::Json,
            requires_auth: vec![],
            default_timeout: 60,
        },
        SkillDef {
            id: "qc_analysis".into(),
            name: "Video QC Analysis".into(),
            description: "AI-driven quality check: black frames, audio issues, color shifts. Outputs timecoded findings.".into(),
            keywords: vec!["qc".into(), "quality".into(), "check".into(), "black frame".into(), "audio dropout".into(), "color shift".into(), "ingest".into()],
            tools: vec!["ffprobe".into(), "ffmpeg".into(), "vllm".into()],
            output_type: OutputType::TimecodedNotes,
            requires_auth: vec![],
            default_timeout: 300,
        },
        SkillDef {
            id: "loudness_scan".into(),
            name: "Loudness Scan".into(),
            description: "EBU R128 loudness analysis for broadcast compliance".into(),
            keywords: vec!["loudness".into(), "audio".into(), "ebu".into(), "r128".into(), "compliance".into()],
            tools: vec!["ffmpeg".into()],
            output_type: OutputType::Json,
            requires_auth: vec![],
            default_timeout: 120,
        },
        SkillDef {
            id: "scene_detect".into(),
            name: "Scene Detection".into(),
            description: "Detect scene cuts and transitions with timecodes".into(),
            keywords: vec!["scene".into(), "cut".into(), "transition".into(), "edit point".into()],
            tools: vec!["ffmpeg".into()],
            output_type: OutputType::Json,
            requires_auth: vec![],
            default_timeout: 120,
        },
    ]
}

pub struct FfprobeMetadata;
pub struct QcAnalysis;
pub struct LoudnessScan;
pub struct SceneDetect;

// ============================================================================
// FFprobe Metadata
// ============================================================================

#[async_trait::async_trait]
impl SkillExecutor for FfprobeMetadata {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let video_uri = ctx.video_uri.as_deref()
            .or_else(|| extract_url_from_content(&ctx.cell_content))
            .ok_or_else(|| anyhow!("No video URI provided"))?
            .to_string();
        
        tracing::info!("🔧 [ffprobe] Analyzing: {}", &video_uri[..video_uri.len().min(60)]);
        
        let output = run_ffprobe(&video_uri).await?;
        
        Ok(SkillResult {
            skill_id: "ffprobe_metadata".into(),
            output_type: OutputType::Json,
            summary: format!("Video metadata extracted: {}", summarize_ffprobe(&output)),
            data: output,
            timecoded_findings: None,
            action_taken: None,
            artifacts: vec![],
        })
    }
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }
}

// ============================================================================
// QC Analysis (the star — generates timecoded video notes)
// ============================================================================

#[async_trait::async_trait]
impl SkillExecutor for QcAnalysis {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let video_uri = ctx.video_uri.as_deref()
            .or_else(|| extract_url_from_content(&ctx.cell_content))
            .ok_or_else(|| anyhow!("No video URI provided"))?
            .to_string();
        
        tracing::info!("🔧 [qc_analysis] Running QC on: {}", &video_uri[..video_uri.len().min(60)]);
        
        // Step 1: Get video metadata
        let metadata = run_ffprobe(&video_uri).await.unwrap_or(json!({}));
        let duration = metadata["format"]["duration"].as_str()
            .and_then(|d| d.parse::<f64>().ok())
            .unwrap_or(0.0);
        
        // Step 2: Run black frame detection
        let black_frames = run_black_frame_detect(&video_uri).await.unwrap_or_default();
        
        // Step 3: Run loudness analysis
        let loudness = run_loudness_scan(&video_uri).await.unwrap_or(json!({}));
        
        // Step 4: Send everything to AI for analysis
        let prompt = format!(
            "You are a video QC analyst. Analyze this video and generate a JSON array of timecoded findings.\n\n\
             Video duration: {:.1}s\n\
             Metadata: {}\n\
             Black frames detected: {}\n\
             Loudness data: {}\n\
             User QC instructions: {}\n\n\
             For each issue found, return a JSON object with:\n\
             - timecode_seconds (float)\n\
             - content (description of the issue)\n\
             - finding_type: \"qc_issue\" or \"comment\"\n\
             - severity: \"info\", \"warning\", or \"critical\"\n\
             - source: which analysis tool found it\n\
             - suggested_action: what to do about it (or null)\n\n\
             Generate realistic findings based on the actual metadata. \
             If the video looks clean, say so but still note any edge cases.\n\
             Return ONLY a JSON array, no other text.",
            duration,
            serde_json::to_string_pretty(&metadata).unwrap_or_default(),
            serde_json::to_string(&black_frames).unwrap_or_default(),
            serde_json::to_string_pretty(&loudness).unwrap_or_default(),
            ctx.cell_content,
        );
        
        let response = crate::pipeline::call_vllm_public(&prompt, 1000, 0.3).await?;
        
        // Parse AI response as findings
        let cleaned = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        let findings: Vec<Finding> = serde_json::from_str(cleaned).unwrap_or_else(|e| {
            tracing::warn!("Failed to parse QC findings JSON: {}. Raw: {}", e, &cleaned[..cleaned.len().min(200)]);
            // Generate a single finding from the raw response
            vec![Finding {
                timecode_seconds: 0.0,
                content: response.clone(),
                finding_type: "comment".into(),
                severity: "info".into(),
                source: "qc_analysis".into(),
                suggested_action: None,
            }]
        });
        
        tracing::info!("🔧 [qc_analysis] Generated {} timecoded findings", findings.len());
        
        let summary = format!(
            "QC complete: {} findings ({} critical, {} warnings)",
            findings.len(),
            findings.iter().filter(|f| f.severity == "critical").count(),
            findings.iter().filter(|f| f.severity == "warning").count(),
        );
        
        Ok(SkillResult {
            skill_id: "qc_analysis".into(),
            output_type: OutputType::TimecodedNotes,
            summary,
            data: json!({
                "duration": duration,
                "finding_count": findings.len(),
                "metadata_summary": summarize_ffprobe(&metadata),
            }),
            timecoded_findings: Some(findings),
            action_taken: None,
            artifacts: vec![],
        })
    }
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[1].clone())
    }
}

// ============================================================================
// Loudness Scan
// ============================================================================

#[async_trait::async_trait]
impl SkillExecutor for LoudnessScan {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let video_uri = ctx.video_uri.as_deref()
            .or_else(|| extract_url_from_content(&ctx.cell_content))
            .ok_or_else(|| anyhow!("No video URI"))?
            .to_string();
        
        let loudness = run_loudness_scan(&video_uri).await?;
        
        Ok(SkillResult {
            skill_id: "loudness_scan".into(),
            output_type: OutputType::Json,
            summary: format!("Loudness: {}", loudness),
            data: loudness,
            timecoded_findings: None, action_taken: None, artifacts: vec![],
        })
    }
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[2].clone())
    }
}

// ============================================================================
// Scene Detection  
// ============================================================================

#[async_trait::async_trait]
impl SkillExecutor for SceneDetect {
    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let video_uri = ctx.video_uri.as_deref()
            .or_else(|| extract_url_from_content(&ctx.cell_content))
            .ok_or_else(|| anyhow!("No video URI"))?
            .to_string();
        
        let output = run_scene_detect(&video_uri).await?;
        
        Ok(SkillResult {
            skill_id: "scene_detect".into(),
            output_type: OutputType::Json,
            summary: format!("{} scenes detected", output.as_array().map(|a| a.len()).unwrap_or(0)),
            data: output,
            timecoded_findings: None, action_taken: None, artifacts: vec![],
        })
    }
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[3].clone())
    }
}

// ============================================================================
// Tool Runners (actual ffprobe/ffmpeg calls)
// ============================================================================

async fn run_ffprobe(video_uri: &str) -> Result<serde_json::Value> {
    let output = tokio::process::Command::new("ffprobe")
        .args([
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            video_uri,
        ])
        .output()
        .await
        .map_err(|e| anyhow!("ffprobe not found or failed: {}. Install with: brew install ffmpeg", e))?;
    
    if output.status.success() {
        let json_str = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(&json_str)
            .map_err(|e| anyhow!("Failed to parse ffprobe output: {}", e))
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!("ffprobe failed: {}", stderr))
    }
}

async fn run_black_frame_detect(video_uri: &str) -> Result<Vec<serde_json::Value>> {
    let output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i", video_uri,
            "-vf", "blackdetect=d=0.05:pix_th=0.10",
            "-an", "-f", "null", "-",
        ])
        .output()
        .await
        .map_err(|e| anyhow!("ffmpeg blackdetect failed: {}", e))?;
    
    // Parse stderr for blackdetect output
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut frames = Vec::new();
    
    for line in stderr.lines() {
        if line.contains("black_start:") {
            let start = extract_float_after(line, "black_start:");
            let end = extract_float_after(line, "black_end:");
            let duration = extract_float_after(line, "black_duration:");
            
            if let Some(s) = start {
                frames.push(json!({
                    "start": s,
                    "end": end,
                    "duration": duration,
                }));
            }
        }
    }
    
    Ok(frames)
}

async fn run_loudness_scan(video_uri: &str) -> Result<serde_json::Value> {
    let output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i", video_uri,
            "-af", "loudnorm=print_format=json",
            "-f", "null", "-",
        ])
        .output()
        .await
        .map_err(|e| anyhow!("ffmpeg loudnorm failed: {}", e))?;
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    
    // Extract JSON from stderr (it's embedded in log output)
    if let Some(json_start) = stderr.rfind('{')
        && let Some(json_end) = stderr.rfind('}') {
            let json_str = &stderr[json_start..=json_end];
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) {
                return Ok(val);
            }
        }
    
    Ok(json!({"error": "Could not parse loudness data"}))
}

async fn run_scene_detect(video_uri: &str) -> Result<serde_json::Value> {
    let output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i", video_uri,
            "-filter:v", "select='gt(scene,0.3)',showinfo",
            "-f", "null", "-",
        ])
        .output()
        .await
        .map_err(|e| anyhow!("ffmpeg scene detect failed: {}", e))?;
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut scenes = Vec::new();
    
    for line in stderr.lines() {
        if line.contains("pts_time:")
            && let Some(tc) = extract_float_after(line, "pts_time:") {
                scenes.push(json!({"timecode": tc}));
            }
    }
    
    Ok(json!(scenes))
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_float_after(text: &str, prefix: &str) -> Option<f64> {
    text.find(prefix).and_then(|idx| {
        let start = idx + prefix.len();
        let rest = &text[start..];
        let end = rest.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(rest.len());
        rest[..end].trim().parse().ok()
    })
}

fn extract_url_from_content(content: &str) -> Option<&str> {
    // Find first http URL in cell content
    content.split_whitespace().find(|&word| word.starts_with("http://") || word.starts_with("https://") || word.starts_with("s3://")).map(|v| v as _)
}

fn summarize_ffprobe(metadata: &serde_json::Value) -> String {
    let format = &metadata["format"];
    let duration = format["duration"].as_str().unwrap_or("?");
    let size = format["size"].as_str().unwrap_or("?");
    let format_name = format["format_long_name"].as_str().unwrap_or("unknown");
    
    let video_stream = metadata["streams"].as_array()
        .and_then(|streams| streams.iter().find(|s| s["codec_type"].as_str() == Some("video")));
    
    let resolution = video_stream
        .map(|s| format!("{}x{}", s["width"], s["height"]))
        .unwrap_or_else(|| "?".into());
    
    let codec = video_stream
        .and_then(|s| s["codec_name"].as_str())
        .unwrap_or("?");
    
    format!("{} | {} | {} | {}s | {} bytes", format_name, codec, resolution, duration, size)
}

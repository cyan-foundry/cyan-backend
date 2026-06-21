// cyan-backend/src/skills/qc_fix.rs
//
// QC Fix Skill — executes user-requested video corrections.
// When a user flags an issue on the timeline ("saturation too low at 3:20",
// "audio pop at 4:15", "too dark in this scene"), Lens maps the
// issue to the appropriate ffmpeg filter and applies it.
//
// This is the human-AI collaboration loop:
//   1. QC Analysis detects issues → timecoded notes
//   2. User reviews and flags additional issues
//   3. QC Fix executes the corrections
//   4. User reviews the result
//
// ENCODING RULES (from pipeline testing):
//   - Always re-encode full output with -vsync cfr -movflags +faststart -pix_fmt yuv420p
//   - Never use -c copy for segments that need filter processing
//   - All outputs: libx264 preset medium, crf 20, aac 44100hz stereo

use anyhow::{anyhow, Result};
use crate::skills::{SkillExecutor, SkillDef, SkillContext, SkillResult, OutputType, Finding, InferenceStatus};

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "qc_fix".into(),
            name: "QC Fix Execution".into(),
            description: "Execute video corrections: color grading, audio fixes, cropping, deinterlacing. Maps user complaints to ffmpeg filters.".into(),
            keywords: vec![
                "fix".into(), "correct".into(), "adjust".into(), "saturation".into(),
                "brightness".into(), "contrast".into(), "color".into(), "dark".into(),
                "bright".into(), "audio".into(), "pop".into(), "click".into(),
                "crop".into(), "deinterlace".into(), "stabilize".into(), "sharpen".into(),
                "denoise".into(), "volume".into(), "loudness".into(), "level".into(),
            ],
            tools: vec!["ffmpeg".into(), "vllm".into()],
            output_type: OutputType::Summary,
            requires_auth: vec![],
            default_timeout: 600,
        },
    ]
}

fn emit(step_id: &str, model: &str, phase: &str, message: &str) {
    let status = InferenceStatus::new(step_id, model, phase, message);
    eprintln!("{}", status.display());
}

pub struct QcFix;

#[async_trait::async_trait]
impl SkillExecutor for QcFix {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let step_id = &ctx.step_id;
        let video_uri = ctx.video_uri.as_deref()
            .ok_or_else(|| anyhow!("No video URI for QC fix"))?;

        // ── Step 1: Ask LLM to map the user complaint to ffmpeg filters ──
        emit(step_id, "llama-3.3-70b", "analyzing", 
            "Analyzing QC issue and determining corrective ffmpeg filters...");

        // Gather all QC findings from previous steps
        let qc_context = ctx.previous_outputs.iter()
            .filter(|o| o.step_id.contains("qc") || o.step_id.contains("quality"))
            .map(|o| format!("[{}]: {}", o.step_id, &o.output[..o.output.len().min(500)]))
            .collect::<Vec<_>>()
            .join("\n");

        let prompt = format!(r#"You are a broadcast video engineer. A QC reviewer has flagged an issue that needs fixing.

VIDEO: {video_uri}
USER COMPLAINT: {complaint}
PREVIOUS QC FINDINGS: {qc_context}

Map this complaint to the correct ffmpeg filter(s). Return ONLY a JSON response:
{{
    "diagnosis": "what the issue is",
    "filters": {{
        "video_filter": "the -vf filter string or null",
        "audio_filter": "the -af filter string or null"
    }},
    "timecode_range": {{
        "start": null or start_seconds,
        "end": null or end_seconds
    }},
    "apply_to": "full_video or segment",
    "confidence": 0.0-1.0,
    "notes": "any warnings or caveats"
}}

COMMON FILTER MAPPINGS:
- "too dark" / "brightness" → eq=brightness=0.1 (range -1.0 to 1.0)
- "saturation low/high" → eq=saturation=1.3 (1.0 = normal, 0-3.0 range)
- "contrast" → eq=contrast=1.2 (1.0 = normal)
- "color temperature warm/cool" → colortemperature=6500 (default, higher=warm, lower=cool)
- "audio pop/click" → adeclick (automatic click removal)
- "audio hum/buzz" → anlmdn (noise reduction) or highpass=f=80,lowpass=f=8000
- "volume too low/high" → volume=1.5 (multiplier)
- "loudness normalize" → loudnorm=I=-16:TP=-1.5:LRA=11
- "deinterlace" → yadif
- "stabilize" → vidstabdetect + vidstabtransform (two-pass)
- "sharpen" → unsharp=5:5:0.8
- "denoise" → nlmeans or hqdn3d
- "crop" → crop=w:h:x:y
- "scale" → scale=1920:1080

Return null for filters that don't apply. If the issue requires only audio or only video filters, leave the other as null."#,
            video_uri = video_uri,
            complaint = ctx.cell_content,
            qc_context = if qc_context.is_empty() { "None".to_string() } else { qc_context },
        );

        let response = crate::pipeline::call_vllm_public(&prompt, 800, 0.2).await?;

        // Parse LLM response
        let cleaned = response.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        let fix_plan: serde_json::Value = serde_json::from_str(cleaned).map_err(|e| {
            anyhow!("Failed to parse QC fix plan: {}. Raw: {}", e, &cleaned[..cleaned.len().min(300)])
        })?;

        let diagnosis = fix_plan["diagnosis"].as_str().unwrap_or("Unknown issue").to_string();
        let video_filter = fix_plan["filters"]["video_filter"].as_str().map(|s| s.to_string());
        let audio_filter = fix_plan["filters"]["audio_filter"].as_str().map(|s| s.to_string());
        let confidence = fix_plan["confidence"].as_f64().unwrap_or(0.0);
        let notes = fix_plan["notes"].as_str().unwrap_or("").to_string();

        emit(step_id, "llama-3.3-70b", "complete",
            &format!("Diagnosis: {} (confidence: {:.0}%)", diagnosis, confidence * 100.0));

        if video_filter.is_none() && audio_filter.is_none() {
            return Ok(SkillResult {
                skill_id: "qc_fix".into(),
                summary: format!("No corrective filters needed. Diagnosis: {}", diagnosis),
                output_type: OutputType::Summary,
                data: fix_plan.clone(),
                timecoded_findings: None,
                action_taken: None,
                artifacts: vec![],
            });
        }

        // ── Step 2: Build and execute ffmpeg command ─────────
        emit(step_id, "ffmpeg", "processing", 
            &format!("Applying fix: {}", diagnosis));

        let output_path = "/tmp/pipeline_qc_fixed.mp4";
        let mut cmd_args: Vec<String> = vec![
            "-y".into(), "-i".into(), video_uri.to_string(),
        ];

        // Add time range if specified
        let start_time = fix_plan["timecode_range"]["start"].as_f64();
        let end_time = fix_plan["timecode_range"]["end"].as_f64();
        
        if let Some(ss) = start_time {
            cmd_args.push("-ss".into());
            cmd_args.push(format!("{}", ss));
        }
        if let (Some(ss), Some(to)) = (start_time, end_time) {
            cmd_args.push("-t".into());
            cmd_args.push(format!("{}", to - ss));
        }

        // Add filters
        if let Some(ref vf) = video_filter {
            cmd_args.push("-vf".into());
            cmd_args.push(vf.clone());
        }
        if let Some(ref af) = audio_filter {
            cmd_args.push("-af".into());
            cmd_args.push(af.clone());
        }

        // ENCODING RULES — always apply these
        cmd_args.extend([
            "-c:v".into(), "libx264".into(),
            "-preset".into(), "medium".into(),
            "-crf".into(), "20".into(),
            "-c:a".into(), "aac".into(),
            "-ar".into(), "44100".into(),
            "-ac".into(), "2".into(),
            "-b:a".into(), "192k".into(),
            "-vsync".into(), "cfr".into(),
            "-movflags".into(), "+faststart".into(),
            "-pix_fmt".into(), "yuv420p".into(),
            output_path.into(),
        ]);

        let cmd_str: Vec<&str> = cmd_args.iter().map(|s| s.as_str()).collect();
        let ffmpeg_result = tokio::process::Command::new("ffmpeg")
            .args(&cmd_str)
            .output()
            .await
            .map_err(|e| anyhow!("ffmpeg QC fix failed: {}", e))?;

        if !ffmpeg_result.status.success() {
            let stderr = String::from_utf8_lossy(&ffmpeg_result.stderr);
            return Err(anyhow!("ffmpeg fix failed: {}", &stderr[..stderr.len().min(500)]));
        }

        emit(step_id, "ffmpeg", "complete", 
            &format!("Fix applied successfully → {}", output_path));

        let summary = format!(
            "QC Fix applied: {}\n\
             Video filter: {}\n\
             Audio filter: {}\n\
             Confidence: {:.0}%\n\
             Notes: {}\n\
             Output: {}",
            diagnosis,
            video_filter.as_deref().unwrap_or("none"),
            audio_filter.as_deref().unwrap_or("none"),
            confidence * 100.0,
            notes,
            output_path
        );

        Ok(SkillResult {
            skill_id: "qc_fix".into(),
            summary,
            output_type: OutputType::Summary,
            data: fix_plan.clone(),
            timecoded_findings: Some(vec![Finding {
                timecode_seconds: fix_plan["timecode_range"]["start"].as_f64().unwrap_or(0.0),
                content: format!("✅ Fix applied: {}", diagnosis),
                finding_type: "qc_fix".to_string(),
                severity: "info".to_string(),
                source: "qc_fix/ffmpeg".to_string(),
                suggested_action: Some("Review corrected output".to_string()),
            }]),
            action_taken: Some(format!("Applied ffmpeg filters: vf={}, af={}", 
                video_filter.as_deref().unwrap_or("none"), audio_filter.as_deref().unwrap_or("none"))),
            artifacts: vec![output_path.to_string()],
        })
    }
}

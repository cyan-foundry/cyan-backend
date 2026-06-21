// cyan-backend/src/skills/ad_splice.rs
//
// SSAI Ad Splice Skill — insert commercial breaks with proper encoding.
//
// CRITICAL ENCODING RULES (learned from pipeline testing):
//   1. ALWAYS check if ad has audio track → add anullsrc if missing
//   2. ALWAYS normalize ALL segments to identical format before concat:
//      - Same resolution, framerate (24fps), GOP (48), pix_fmt (yuv420p)
//      - Same audio: aac, 44100hz, stereo, 192k
//   3. ALWAYS final re-encode with -vsync cfr -movflags +faststart
//   4. NEVER use -c copy concat — it causes stutter at transition points
//   5. Adjust all downstream timecodes (bleeps, subtitles) by cumulative ad duration

use anyhow::{anyhow, Result};
use serde_json::json;
use crate::skills::{SkillExecutor, SkillDef, SkillContext, SkillResult, OutputType, Finding, InferenceStatus};

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "ad_splice".into(),
            name: "Ad Splice & Package".into(),
            description: "Insert commercial breaks at SSAI-detected points with proper encoding normalization, bleep application, and final packaging.".into(),
            keywords: vec![
                "splice".into(), "insert".into(), "ad".into(), "commercial".into(),
                "break".into(), "package".into(), "final".into(), "deliver".into(),
                "concat".into(), "merge".into(), "bleep".into(), "censor".into(),
            ],
            tools: vec!["ffmpeg".into(), "ffprobe".into()],
            output_type: OutputType::Summary,
            requires_auth: vec![],
            default_timeout: 900,
        },
    ]
}

fn emit(step_id: &str, model: &str, phase: &str, message: &str) {
    let status = InferenceStatus::new(step_id, model, phase, message);
    eprintln!("{}", status.display());
}

pub struct AdSplice;

#[async_trait::async_trait]
impl SkillExecutor for AdSplice {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let step_id = &ctx.step_id;
        let video_uri = ctx.video_uri.as_deref()
            .ok_or_else(|| anyhow!("No video URI for ad splice"))?;

        // ── Load ad break points ─────────────────────────────
        emit(step_id, "pipeline", "loading", "Loading SSAI ad break points...");
        
        let ad_breaks_data: serde_json::Value = if let Ok(data) = 
            tokio::fs::read_to_string("/tmp/pipeline_ad_breaks.json").await {
            serde_json::from_str(&data).unwrap_or(json!({}))
        } else {
            // Get from previous step
            ctx.previous_outputs.iter()
                .find(|o| o.step_id.contains("ssai"))
                .and_then(|o| serde_json::from_str(&o.output).ok())
                .unwrap_or(json!({}))
        };

        let break_points: Vec<f64> = ad_breaks_data["ad_breaks"].as_array()
            .map(|arr| arr.iter()
                .filter_map(|b| b["timecode_seconds"].as_f64())
                .collect())
            .unwrap_or_default();

        if break_points.is_empty() {
            return Err(anyhow!("No ad break points found. Run SSAI detection first."));
        }

        // ── Get video metadata ───────────────────────────────
        emit(step_id, "ffprobe", "processing", "Analyzing main video format...");
        
        let probe = tokio::process::Command::new("ffprobe")
            .args(["-v", "quiet", "-print_format", "json", "-show_streams", video_uri])
            .output().await?;
        let probe_json: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap_or(json!({}));
        
        let (width, height, fps) = extract_video_params(&probe_json);
        emit(step_id, "ffprobe", "complete", 
            &format!("Video: {}x{} @ {}fps", width, height, fps));

        // ── Find ad clips ────────────────────────────────────
        // Look for ad files in /tmp/ or from cell content
        let ad_clips = find_ad_clips(&ctx.cell_content).await;
        if ad_clips.is_empty() {
            return Err(anyhow!("No ad clips found. Place ad files in /tmp/ or specify in cell content."));
        }

        emit(step_id, "pipeline", "processing", 
            &format!("{} break points, {} ad clips available", break_points.len(), ad_clips.len()));

        // ── RULE 1: Check if ads have audio, add silent if missing ──
        let mut prepared_ads = Vec::new();
        for (i, ad_path) in ad_clips.iter().enumerate() {
            emit(step_id, "ffprobe", "processing", 
                &format!("Checking ad clip {} format...", i + 1));
            
            let has_audio = check_has_audio(ad_path).await;
            
            if has_audio {
                prepared_ads.push(ad_path.clone());
            } else {
                emit(step_id, "ffmpeg", "processing", 
                    &format!("Ad clip {} has no audio — adding silent track...", i + 1));
                
                let fixed_path = format!("/tmp/ad_{}_with_audio.mp4", i);
                let fix_result = tokio::process::Command::new("ffmpeg")
                    .args([
                        "-y", "-i", ad_path,
                        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
                        "-c:v", "copy", "-c:a", "aac", "-shortest",
                        &fixed_path,
                    ])
                    .output().await?;
                
                if fix_result.status.success() {
                    prepared_ads.push(fixed_path);
                } else {
                    return Err(anyhow!("Failed to add audio to ad clip {}", i));
                }
            }
        }

        // ── RULE 2: Normalize ALL segments to identical format ──
        emit(step_id, "ffmpeg", "processing", 
            "Normalizing all segments to identical format (resolution, fps, GOP, audio)...");
        
        let target_fps = fps.max(24);
        let target_gop = target_fps * 2;  // 2 second GOP
        let mut normalized_files = Vec::new();
        let mut cumulative_ad_duration: f64 = 0.0;
        let mut ad_idx = 0;

        // Build segment list: [seg1, ad1, seg2, ad2, seg3, ...]
        let mut prev_point: f64 = 0.0;
        for (i, &bp) in break_points.iter().enumerate() {
            // Main video segment
            let seg_path = format!("/tmp/norm_seg_{}.mp4", i);
            let duration = bp - prev_point;
            
            emit(step_id, "ffmpeg", "processing", 
                &format!("Encoding segment {} ({:.0}s-{:.0}s)...", i + 1, prev_point, bp));
            
            normalize_segment(video_uri, &seg_path, prev_point, duration, 
                width, height, target_fps, target_gop).await?;
            normalized_files.push(seg_path);

            // Ad clip
            if ad_idx < prepared_ads.len() {
                let norm_ad_path = format!("/tmp/norm_ad_{}.mp4", i);
                
                emit(step_id, "ffmpeg", "processing", 
                    &format!("Encoding ad clip {} to match video format...", i + 1));
                
                normalize_segment(&prepared_ads[ad_idx], &norm_ad_path, 0.0, -1.0,
                    width, height, target_fps, target_gop).await?;
                
                // Get ad duration for timecode adjustment
                let ad_dur = get_duration(&norm_ad_path).await;
                cumulative_ad_duration += ad_dur;
                
                normalized_files.push(norm_ad_path);
                ad_idx += 1;
            }

            prev_point = bp;
        }

        // Final segment (after last break point to end)
        let final_seg_path = "/tmp/norm_seg_final.mp4".to_string();
        emit(step_id, "ffmpeg", "processing", 
            &format!("Encoding final segment ({:.0}s to end)...", prev_point));
        normalize_segment(video_uri, &final_seg_path, prev_point, -1.0,
            width, height, target_fps, target_gop).await?;
        normalized_files.push(final_seg_path);

        // ── Concat all normalized segments ───────────────────
        emit(step_id, "ffmpeg", "processing", 
            &format!("Concatenating {} segments...", normalized_files.len()));
        
        let concat_list_path = "/tmp/pipeline_concat.txt";
        let concat_content: String = normalized_files.iter()
            .map(|f| format!("file {}", f))
            .collect::<Vec<_>>()
            .join("\n");
        tokio::fs::write(concat_list_path, &concat_content).await?;

        let concat_output = "/tmp/pipeline_spliced.mp4";
        let concat_result = tokio::process::Command::new("ffmpeg")
            .args([
                "-y", "-f", "concat", "-safe", "0",
                "-i", concat_list_path,
                "-c", "copy",
                "-movflags", "+faststart",
                concat_output,
            ])
            .output().await?;

        if !concat_result.status.success() {
            return Err(anyhow!("Concat failed: {}", 
                String::from_utf8_lossy(&concat_result.stderr)));
        }

        // ── RULE 3: Final re-encode for universal playback ───
        emit(step_id, "ffmpeg", "processing", 
            "Final re-encode for universal playback (Slack, Telegram, web)...");
        
        let final_output = "/tmp/pipeline_final.mp4";
        let final_result = tokio::process::Command::new("ffmpeg")
            .args([
                "-y", "-i", concat_output,
                "-c:v", "libx264", "-preset", "medium", "-crf", "22",
                "-c:a", "aac", "-b:a", "192k",
                "-movflags", "+faststart",
                "-pix_fmt", "yuv420p",
                "-vsync", "cfr",
                final_output,
            ])
            .output().await?;

        if !final_result.status.success() {
            return Err(anyhow!("Final encode failed: {}", 
                String::from_utf8_lossy(&final_result.stderr)));
        }

        let final_duration = get_duration(final_output).await;
        let final_size = tokio::fs::metadata(final_output).await
            .map(|m| m.len() as f64 / 1024.0 / 1024.0)
            .unwrap_or(0.0);

        emit(step_id, "ffmpeg", "complete", 
            &format!("Final video: {:.1}MB, {:.0}s ({} ad breaks inserted)", 
                final_size, final_duration, break_points.len()));

        // ── Save timecode adjustment data for bleeps + subtitles ──
        let adjustment_data = json!({
            "break_points": break_points,
            "ad_durations": cumulative_ad_duration,
            "note": "All timecodes after ad insertion points must be shifted forward by cumulative ad duration"
        });
        let _ = tokio::fs::write("/tmp/pipeline_timecode_adjustments.json",
            serde_json::to_string_pretty(&adjustment_data).unwrap_or_default()).await;

        let summary = format!(
            "Ad splice complete:\n\
             Ads inserted at: {}\n\
             Total ad duration: {:.1}s\n\
             Final video: {:.1}MB, {:.0}s\n\
             Output: {}\n\
             Timecode adjustments saved for downstream steps",
            break_points.iter().map(|bp| {
                let m = (*bp as u32) / 60;
                let s = (*bp as u32) % 60;
                format!("{}:{:02}", m, s)
            }).collect::<Vec<_>>().join(", "),
            cumulative_ad_duration,
            final_size, final_duration,
            final_output
        );

        Ok(SkillResult {
            skill_id: "ad_splice".into(),
            summary,
            output_type: OutputType::Summary,
            data: json!({
                "output_path": final_output,
                "duration": final_duration,
                "size_mb": final_size,
                "break_points": break_points,
                "total_ad_duration": cumulative_ad_duration,
            }),
            timecoded_findings: Some(break_points.iter().enumerate().map(|(i, bp)| {
                Finding {
                    timecode_seconds: *bp,
                    content: format!("📺 Ad break {} inserted at {}:{:02}", i + 1, (*bp as u32) / 60, (*bp as u32) % 60),
                    finding_type: "ad_insertion".to_string(),
                    severity: "info".to_string(),
                    source: "ad_splice/ffmpeg".to_string(),
                    suggested_action: None,
                }
            }).collect()),
            action_taken: Some(format!("Inserted {} ad breaks", break_points.len())),
            artifacts: vec![final_output.to_string()],
        })
    }
}

// ============================================================================
// Helpers
// ============================================================================

async fn check_has_audio(path: &str) -> bool {
    let output = tokio::process::Command::new("ffprobe")
        .args(["-v", "quiet", "-show_entries", "stream=codec_type", "-of", "csv=p=0", path])
        .output().await;
    output.map(|o| String::from_utf8_lossy(&o.stdout).contains("audio")).unwrap_or(false)
}

fn extract_video_params(probe: &serde_json::Value) -> (u32, u32, u32) {
    let video = probe["streams"].as_array()
        .and_then(|s| s.iter().find(|s| s["codec_type"].as_str() == Some("video")));
    
    let width = video.and_then(|v| v["width"].as_u64()).unwrap_or(1920) as u32;
    let height = video.and_then(|v| v["height"].as_u64()).unwrap_or(800) as u32;
    let fps_str = video.and_then(|v| v["r_frame_rate"].as_str()).unwrap_or("24/1");
    let fps: u32 = fps_str.split('/').next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(24);
    
    (width, height, fps)
}

async fn normalize_segment(
    input: &str, output: &str, 
    start: f64, duration: f64,
    width: u32, height: u32, fps: u32, gop: u32,
) -> Result<()> {
    let mut args: Vec<String> = vec!["-y".into()];
    
    if start > 0.0 {
        args.push("-ss".into());
        args.push(format!("{}", start));
    }
    
    args.push("-i".into());
    args.push(input.into());
    
    if duration > 0.0 {
        args.push("-t".into());
        args.push(format!("{}", duration));
    }

    // Check if input has audio
    let has_audio = check_has_audio(input).await;
    if !has_audio {
        args.extend(["-f".into(), "lavfi".into(), "-i".into(), "anullsrc=r=44100:cl=stereo".into()]);
        args.push("-shortest".into());
    }

    args.extend([
        "-vf".into(), format!("scale={}:{}:force_original_aspect_ratio=decrease,pad={}:{}:(ow-iw)/2:(oh-ih)/2", 
            width, height, width, height),
        "-c:v".into(), "libx264".into(),
        "-preset".into(), "fast".into(),
        "-crf".into(), "20".into(),
        "-r".into(), format!("{}", fps),
        "-g".into(), format!("{}", gop),
        "-c:a".into(), "aac".into(),
        "-ar".into(), "44100".into(),
        "-ac".into(), "2".into(),
        "-b:a".into(), "192k".into(),
        "-video_track_timescale".into(), format!("{}000", fps),
        "-pix_fmt".into(), "yuv420p".into(),
        output.into(),
    ]);

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let result = tokio::process::Command::new("ffmpeg")
        .args(&arg_refs)
        .output().await
        .map_err(|e| anyhow!("ffmpeg normalize failed: {}", e))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(anyhow!("Normalize failed for {}: {}", output, &stderr[..stderr.len().min(300)]));
    }

    Ok(())
}

async fn get_duration(path: &str) -> f64 {
    let output = tokio::process::Command::new("ffprobe")
        .args(["-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", path])
        .output().await;
    output.ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0.0)
}

async fn find_ad_clips(cell_content: &str) -> Vec<String> {
    let mut clips = Vec::new();
    
    // Check /tmp/ for ad files
    for pattern in &["/tmp/ad_chips*.mp4", "/tmp/ad_cocacola*.mp4", "/tmp/ad_*.mp4"] {
        if let Ok(entries) = glob::glob(pattern) {
            for entry in entries.flatten() {
                let path = entry.to_string_lossy().to_string();
                if !clips.contains(&path) {
                    clips.push(path);
                }
            }
        }
    }
    
    // Also check cell content for paths/URLs
    for word in cell_content.split_whitespace() {
        if (word.ends_with(".mp4") || word.ends_with(".mov")) 
            && (word.contains("ad") || word.contains("commercial")) {
            clips.push(word.to_string());
        }
    }

    clips
}

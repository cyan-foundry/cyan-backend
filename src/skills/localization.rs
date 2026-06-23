// cyan-backend/src/skills/localization.rs
//
// Translation, Compliance, and Transcription skills
// Real tool execution with inference status feedback:
//   - Whisper Large V3 for transcription (via faster-whisper)
//   - IndicTrans2 1B for Indian language translation
//   - Llama 70B for compliance + cultural adaptation
//   - Word-level timestamps for precise bleep placement
//
// ENCODING RULES (baked in from pipeline testing):
//   - Always use word_timestamps=True for Whisper
//   - All ffmpeg outputs: -vsync cfr -movflags +faststart -pix_fmt yuv420p
//   - Normalize all segments before concat: same fps, GOP, audio format
//   - Check if ads have audio tracks, add anullsrc if missing
//   - Adjust all timecodes after ad insertion by cumulative ad duration

#![allow(dead_code)] // Skills scaffolding moving to the MCP/workflow model; see CLAUDE.md 'Out of scope'.

use anyhow::{anyhow, Result};
use serde_json::json;
use crate::skills::{SkillExecutor, SkillDef, SkillContext, SkillResult, OutputType, Finding, InferenceStatus};

pub fn register() -> Vec<SkillDef> {
    vec![
        SkillDef {
            id: "transcription".into(),
            name: "Audio Transcription".into(),
            description: "Transcribe audio/dialogue from video using Whisper Large V3 with word-level timestamps".into(),
            keywords: vec![
                "transcri".into(), "whisper".into(), "dialogue".into(), "speech".into(),
                "stt".into(), "subtitle".into(), "caption".into(), "audio".into(),
                "speaker".into(),
            ],
            tools: vec!["whisper".into(), "ffmpeg".into()],
            output_type: OutputType::Summary,
            requires_auth: vec![],
            default_timeout: 900,
        },
        SkillDef {
            id: "translation".into(),
            name: "Multi-language Translation".into(),
            description: "Translate transcript to Indian languages using IndicTrans2 1B with cultural adaptation via Llama 70B".into(),
            keywords: vec![
                "translat".into(), "hindi".into(), "tamil".into(), "telugu".into(),
                "kannada".into(), "malayalam".into(), "language".into(), "locali".into(),
                "dub".into(), "subtitle".into(),
            ],
            tools: vec!["indictrans2".into(), "vllm".into()],
            output_type: OutputType::Summary,
            requires_auth: vec![],
            default_timeout: 600,
        },
        SkillDef {
            id: "compliance_check".into(),
            name: "Territory Compliance Check".into(),
            description: "Check content against broadcast regulations (CBFC, IMDA, NMC) with word-level profanity detection for precise bleeping".into(),
            keywords: vec![
                "compliance".into(), "cbfc".into(), "imda".into(), "regulat".into(),
                "tobacco".into(), "censor".into(), "restrict".into(), "territory".into(),
                "watershed".into(), "rating".into(), "profanity".into(), "bleep".into(),
            ],
            tools: vec!["whisper".into(), "vllm".into(), "ffmpeg".into()],
            output_type: OutputType::TimecodedNotes,
            requires_auth: vec![],
            default_timeout: 600,
        },
    ]
}

// ============================================================================
// Inference Status Helper — feeds real-time UI updates
// ============================================================================

fn emit(step_id: &str, model: &str, phase: &str, message: &str) {
    let status = InferenceStatus::new(step_id, model, phase, message);
    // This goes to journalctl + StatusMarker in Lens response
    eprintln!("{}", status.display());
}

fn emit_progress(step_id: &str, model: &str, phase: &str, message: &str, progress: f32) {
    let status = InferenceStatus::new(step_id, model, phase, message).with_progress(progress);
    eprintln!("{}", status.display());
}

// ============================================================================
// SRT time formatting
// ============================================================================

fn format_srt_time(seconds: f64) -> String {
    let h = (seconds / 3600.0) as u32;
    let m = ((seconds % 3600.0) / 60.0) as u32;
    let s = seconds % 60.0;
    format!("{:02}:{:02}:{:06.3}", h, m, s)
}

// ============================================================================
// Transcription Skill — Real Whisper + word timestamps
// ============================================================================

pub struct Transcription;

#[async_trait::async_trait]
impl SkillExecutor for Transcription {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[0].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let step_id = &ctx.step_id;
        let video_uri = ctx.video_uri.as_deref()
            .ok_or_else(|| anyhow!("No video URI for transcription"))?;

        // ── Step 1: Extract audio ────────────────────────────
        emit(step_id, "ffmpeg", "processing", "Extracting audio from video...");
        
        let audio_path = "/tmp/pipeline_audio.wav";
        let ffmpeg_result = tokio::process::Command::new("ffmpeg")
            .args(["-y", "-i", video_uri, "-vn", "-ar", "16000", "-ac", "1", "-f", "wav", audio_path])
            .output()
            .await
            .map_err(|e| anyhow!("ffmpeg audio extraction failed: {}", e))?;
        
        if !ffmpeg_result.status.success() {
            return Err(anyhow!("ffmpeg audio extraction failed: {}", 
                String::from_utf8_lossy(&ffmpeg_result.stderr)));
        }
        emit(step_id, "ffmpeg", "complete", "Audio extracted (16kHz mono WAV)");

        // ── Step 2: Run Whisper with word_timestamps=True ────
        // CRITICAL: Always use word_timestamps=True for precise bleep placement later
        emit(step_id, "whisper-large-v3", "loading", "Loading Whisper Large V3 (int8 quantized)...");
        
        let whisper_script = r#"
import json, time, sys
from faster_whisper import WhisperModel

print("WHISPER_STATUS:loading", file=sys.stderr, flush=True)
model = WhisperModel("large-v3", device="cpu", compute_type="int8")

print("WHISPER_STATUS:transcribing", file=sys.stderr, flush=True)
start = time.time()
segments, info = model.transcribe("/tmp/pipeline_audio.wav", beam_size=5, word_timestamps=True)

result = {
    "language": info.language,
    "probability": round(info.language_probability, 3),
    "segments": [],
    "words": [],
    "bleep_candidates": []
}

profanity_list = ["fuck", "shit", "dick", "ass", "bitch", "damn", "hell", "crap", "bastard", "piss"]

for seg in segments:
    seg_data = {"id": len(result["segments"])+1, "start": round(seg.start, 3), "end": round(seg.end, 3), "text": seg.text.strip()}
    result["segments"].append(seg_data)
    
    if seg.words:
        for w in seg.words:
            word_data = {"word": w.word.strip(), "start": round(w.start, 3), "end": round(w.end, 3)}
            result["words"].append(word_data)
            
            # Auto-detect profanity for compliance bleep points
            clean_word = w.word.strip().lower().rstrip(".,!?;:'\"")
            if clean_word in profanity_list:
                result["bleep_candidates"].append({
                    "word": w.word.strip(),
                    "start": round(w.start, 3),
                    "end": round(w.end, 3),
                    "duration": round(w.end - w.start, 3)
                })

result["processing_time"] = round(time.time() - start, 1)
result["segment_count"] = len(result["segments"])
result["word_count"] = len(result["words"])
print(json.dumps(result))
"#;

        emit(step_id, "whisper-large-v3", "processing", 
            "Transcribing audio with word-level timestamps (this may take several minutes on CPU)...");

        let whisper_output = tokio::process::Command::new("python3.11")
            .args(["-c", whisper_script])
            .output()
            .await
            .map_err(|e| anyhow!("Whisper execution failed: {}", e))?;

        if !whisper_output.status.success() {
            let stderr = String::from_utf8_lossy(&whisper_output.stderr);
            return Err(anyhow!("Whisper failed: {}", &stderr[..stderr.len().min(500)]));
        }

        let whisper_json: serde_json::Value = serde_json::from_slice(&whisper_output.stdout)
            .map_err(|e| anyhow!("Failed to parse Whisper JSON: {}", e))?;

        let seg_count = whisper_json["segment_count"].as_u64().unwrap_or(0);
        let word_count = whisper_json["word_count"].as_u64().unwrap_or(0);
        let proc_time = whisper_json["processing_time"].as_f64().unwrap_or(0.0);
        let language = whisper_json["language"].as_str().unwrap_or("unknown");
        let prob = whisper_json["probability"].as_f64().unwrap_or(0.0);
        let bleep_count = whisper_json["bleep_candidates"].as_array().map(|a| a.len()).unwrap_or(0);

        emit(step_id, "whisper-large-v3", "complete", 
            &format!("Transcription complete: {} segments, {} words, {} bleep candidates, language: {} ({:.0}% confidence, {:.0}s)",
                seg_count, word_count, bleep_count, language, prob * 100.0, proc_time));

        // ── Step 3: Build English SRT ────────────────────────
        emit(step_id, "srt-generator", "generating", "Building English SRT subtitle file...");
        
        let mut srt_lines = Vec::new();
        if let Some(segs) = whisper_json["segments"].as_array() {
            for seg in segs {
                let id = seg["id"].as_u64().unwrap_or(0);
                let start = seg["start"].as_f64().unwrap_or(0.0);
                let end = seg["end"].as_f64().unwrap_or(0.0);
                let text = seg["text"].as_str().unwrap_or("");
                if !text.is_empty() && text != "..." && text != "To be continued..." {
                    srt_lines.push(format!("{}\n{} --> {}\n{}\n", 
                        id, format_srt_time(start), format_srt_time(end), text));
                }
            }
        }

        // Save SRT to disk
        let srt_path = "/tmp/pipeline_english.srt";
        if let Err(e) = tokio::fs::write(srt_path, srt_lines.join("\n")).await {
            tracing::warn!("Failed to save SRT: {}", e);
        }

        // Save full transcript JSON (needed by translation + compliance)
        let transcript_path = "/tmp/pipeline_transcript.json";
        if let Err(e) = tokio::fs::write(transcript_path, 
            serde_json::to_string_pretty(&whisper_json).unwrap_or_default()).await {
            tracing::warn!("Failed to save transcript JSON: {}", e);
        }

        let summary = format!(
            "Transcription complete: {} dialogue segments, {} words with word-level timestamps.\n\
             Language: {} (confidence: {:.0}%)\n\
             Profanity auto-detected: {} words\n\
             Processing time: {:.0}s\n\
             Artifacts: {}, {}",
            seg_count, word_count, language, prob * 100.0, 
            bleep_count, proc_time, srt_path, transcript_path
        );

        Ok(SkillResult {
            skill_id: "transcription".into(),
            summary,
            output_type: OutputType::Summary,
            data: whisper_json,
            timecoded_findings: None,
            action_taken: None,
            artifacts: vec![srt_path.to_string(), transcript_path.to_string()],
        })
    }
}

// ============================================================================
// Translation Skill — IndicTrans2 + optional Claude refinement
// ============================================================================

pub struct Translation;

#[async_trait::async_trait]
impl SkillExecutor for Translation {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[1].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let step_id = &ctx.step_id;
        
        // ── Get transcript from previous step ────────────────
        emit(step_id, "pipeline", "loading", "Loading transcript from previous step...");
        
        let transcript_json = ctx.previous_outputs.iter()
            .find(|o| o.step_id.contains("transcri"))
            .and_then(|o| o.artifacts.get("transcript"))
            .cloned();
        
        // Also try loading from disk (pipeline_transcript.json)
        let transcript_data: serde_json::Value = if let Some(tj) = transcript_json {
            tj
        } else if let Ok(data) = tokio::fs::read_to_string("/tmp/pipeline_transcript.json").await {
            serde_json::from_str(&data).unwrap_or(json!({}))
        } else {
            // Fall back to LLM-based translation if no real transcript
            return self.execute_llm_fallback(ctx).await;
        };

        // Extract dialogue lines (filter out empty/placeholder segments)
        let segments = transcript_data["segments"].as_array()
            .ok_or_else(|| anyhow!("No segments in transcript data"))?;
        
        let dialogue_lines: Vec<String> = segments.iter()
            .filter_map(|s| {
                let text = s["text"].as_str()?;
                let text = text.trim();
                if text.is_empty() || text == "..." || text == "To be continued..." {
                    None
                } else {
                    Some(text.to_string())
                }
            })
            .collect();

        if dialogue_lines.is_empty() {
            return Err(anyhow!("No dialogue lines found in transcript"));
        }

        emit(step_id, "indictrans2-1b", "loading", 
            &format!("Loading IndicTrans2 1B model ({} dialogue lines to translate)...", dialogue_lines.len()));

        // ── Determine target languages ───────────────────────
        let content_lower = ctx.cell_content.to_lowercase();
        let mut languages: Vec<(&str, &str)> = Vec::new();
        let lang_map = [
            ("hindi", "hin_Deva"), ("tamil", "tam_Taml"), ("telugu", "tel_Telu"),
            ("kannada", "kan_Knda"), ("malayalam", "mal_Mlym"), ("bengali", "ben_Beng"),
            ("marathi", "mar_Deva"), ("gujarati", "guj_Gujr"),
        ];
        for (name, code) in &lang_map {
            if content_lower.contains(name) {
                languages.push((name, code));
            }
        }
        if languages.is_empty() {
            languages = vec![("hindi", "hin_Deva"), ("tamil", "tam_Taml"), ("telugu", "tel_Telu")];
        }

        // ── Run IndicTrans2 translation ──────────────────────
        let dialogue_json = serde_json::to_string(&dialogue_lines)
            .map_err(|e| anyhow!("Failed to serialize dialogue: {}", e))?;
        
        let lang_codes: Vec<&str> = languages.iter().map(|(_, code)| *code).collect();
        let lang_names: Vec<&str> = languages.iter().map(|(name, _)| *name).collect();
        let lang_codes_json = serde_json::to_string(&lang_codes).unwrap_or_default();
        let lang_names_json = serde_json::to_string(&lang_names).unwrap_or_default();

        // Build segments data for SRT generation
        let segments_json = serde_json::to_string(segments).unwrap_or_default();

        let translation_script = format!(r#"
import torch, time, json, sys

DEVICE = "cpu"
model_path = "/opt/models/indictrans2-en-indic-1B"

lang_codes = {lang_codes_json}
lang_names = {lang_names_json}
dialogue_lines = {dialogue_json}
segments_data = {segments_json}

# Load model
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
from IndicTransToolkit.processor import IndicProcessor

print("TRANSLATE_STATUS:loading_model", file=sys.stderr, flush=True)
tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
model = AutoModelForSeq2SeqLM.from_pretrained(model_path, trust_remote_code=True, torch_dtype=torch.float32).to(DEVICE)
ip = IndicProcessor(inference=True)
print("TRANSLATE_STATUS:model_loaded", file=sys.stderr, flush=True)

results = {{}}
srt_files = {{}}

for tgt_lang, lang_name in zip(lang_codes, lang_names):
    start = time.time()
    print(f"TRANSLATE_STATUS:translating_{{lang_name}}", file=sys.stderr, flush=True)
    
    batch = ip.preprocess_batch(dialogue_lines, src_lang="eng_Latn", tgt_lang=tgt_lang)
    inputs = tokenizer(batch, truncation=True, padding="longest", return_tensors="pt").to(DEVICE)
    with torch.no_grad():
        generated = model.generate(**inputs, num_beams=5, num_return_sequences=1, max_length=256)
    with tokenizer.as_target_tokenizer():
        outputs = tokenizer.batch_decode(generated, skip_special_tokens=True, clean_up_tokenization_spaces=True)
    outputs = ip.postprocess_batch(outputs, lang=tgt_lang)
    
    elapsed = round(time.time() - start, 1)
    results[lang_name] = {{"lines": outputs, "time": elapsed}}
    
    # Build SRT file
    srt_lines = []
    seg_idx = 0
    for i, (seg, text) in enumerate(zip(segments_data, outputs)):
        start_t = seg.get("start", 0)
        end_t = seg.get("end", 0)
        h1, m1, s1 = int(start_t//3600), int((start_t%3600)//60), start_t%60
        h2, m2, s2 = int(end_t//3600), int((end_t%3600)//60), end_t%60
        srt_lines.append(f"{{i+1}}")
        srt_lines.append(f"{{h1:02d}}:{{m1:02d}}:{{s1:06.3f}} --> {{h2:02d}}:{{m2:02d}}:{{s2:06.3f}}")
        srt_lines.append(text)
        srt_lines.append("")
    
    srt_path = f"/tmp/pipeline_{{lang_name}}.srt"
    with open(srt_path, "w") as f:
        f.write("\\n".join(srt_lines))
    srt_files[lang_name] = srt_path
    
    print(f"TRANSLATE_STATUS:{{lang_name}}_done_{{elapsed}}s", file=sys.stderr, flush=True)

output = {{"results": results, "srt_files": srt_files, "languages": lang_names, "line_count": len(dialogue_lines)}}
print(json.dumps(output))
"#);

        for (name, _) in &languages {
            emit(step_id, "indictrans2-1b", "processing", 
                &format!("Translating {} dialogue lines to {}...", dialogue_lines.len(), name));
        }

        let trans_output = tokio::process::Command::new("python3.11")
            .args(["-c", &translation_script])
            .output()
            .await
            .map_err(|e| anyhow!("Translation script failed: {}", e))?;

        if !trans_output.status.success() {
            let stderr = String::from_utf8_lossy(&trans_output.stderr);
            return Err(anyhow!("IndicTrans2 failed: {}", &stderr[..stderr.len().min(500)]));
        }

        let trans_json: serde_json::Value = serde_json::from_slice(&trans_output.stdout)
            .map_err(|e| anyhow!("Failed to parse translation output: {}", e))?;

        let line_count = trans_json["line_count"].as_u64().unwrap_or(0);
        let mut artifacts = Vec::new();
        let mut timing_summary = Vec::new();

        if let Some(results) = trans_json["results"].as_object() {
            for (lang, data) in results {
                let elapsed = data["time"].as_f64().unwrap_or(0.0);
                timing_summary.push(format!("  {} — {} lines in {:.0}s", lang, line_count, elapsed));
                emit(step_id, "indictrans2-1b", "complete",
                    &format!("{} translation complete ({:.0}s)", lang, elapsed));
            }
        }
        if let Some(srt_files) = trans_json["srt_files"].as_object() {
            for (_, path) in srt_files {
                if let Some(p) = path.as_str() {
                    artifacts.push(p.to_string());
                }
            }
        }

        let lang_list = lang_names.join(", ");
        let summary = format!(
            "Translation complete: {} dialogue lines → {} languages ({})\n{}\nSRT files generated: {}",
            line_count, languages.len(), lang_list,
            timing_summary.join("\n"),
            artifacts.join(", ")
        );

        Ok(SkillResult {
            skill_id: "translation".into(),
            summary,
            output_type: OutputType::Summary,
            data: trans_json,
            timecoded_findings: None,
            action_taken: None,
            artifacts,
        })
    }
}

impl Translation {
    /// Fallback: use Llama 70B for translation when IndicTrans2 is unavailable
    async fn execute_llm_fallback(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let step_id = &ctx.step_id;
        emit(step_id, "llama-3.3-70b", "processing", 
            "IndicTrans2 unavailable, falling back to Llama 70B for translation...");

        let transcript = ctx.previous_outputs.iter()
            .find(|o| o.step_id.contains("transcri"))
            .map(|o| o.output.clone())
            .unwrap_or_else(|| "No transcript available".to_string());

        let content_lower = ctx.cell_content.to_lowercase();
        let mut languages = Vec::new();
        for lang in &["hindi", "tamil", "telugu", "kannada", "malayalam"] {
            if content_lower.contains(lang) { languages.push(*lang); }
        }
        if languages.is_empty() { languages = vec!["hindi", "tamil", "telugu"]; }

        let prompt = format!(
            "Translate the following English dialogue to {}. Use native script, not transliteration. \
             Provide translations in SRT format preserving original timecodes.\n\n{}",
            languages.join(", "),
            &transcript[..transcript.len().min(2000)]
        );

        let response = crate::pipeline::call_vllm_public(&prompt, 1500, 0.3).await?;

        emit(step_id, "llama-3.3-70b", "complete", 
            &format!("LLM translation complete for {} languages", languages.len()));

        Ok(SkillResult {
            skill_id: "translation".into(),
            summary: format!("Translation (LLM fallback) to {}: {}", languages.join(", "), 
                &response[..response.len().min(300)]),
            output_type: OutputType::Summary,
            data: json!({"languages": languages, "method": "llm_fallback", "response": response}),
            timecoded_findings: None,
            action_taken: None,
            artifacts: vec![],
        })
    }
}

// ============================================================================
// Compliance Check — LLM analysis + word-level bleep detection
// ============================================================================

pub struct ComplianceCheck;

#[async_trait::async_trait]
impl SkillExecutor for ComplianceCheck {
    fn definition(&self) -> &SkillDef {
        static DEF: std::sync::OnceLock<SkillDef> = std::sync::OnceLock::new();
        DEF.get_or_init(|| register()[2].clone())
    }

    async fn execute(&self, ctx: &SkillContext) -> Result<SkillResult> {
        let step_id = &ctx.step_id;
        
        emit(step_id, "pipeline", "loading", "Loading transcript and word timestamps for compliance analysis...");

        // ── Load transcript with word-level timestamps ───────
        let transcript_data: serde_json::Value = if let Ok(data) = 
            tokio::fs::read_to_string("/tmp/pipeline_transcript.json").await {
            serde_json::from_str(&data).unwrap_or(json!({}))
        } else {
            // Get from previous step output
            ctx.previous_outputs.iter()
                .find(|o| o.step_id.contains("transcri"))
                .and_then(|o| serde_json::from_str(&o.output).ok())
                .unwrap_or(json!({}))
        };

        // ── Auto-detected bleep candidates from Whisper ──────
        let bleep_candidates = transcript_data["bleep_candidates"].as_array()
            .cloned()
            .unwrap_or_default();

        if !bleep_candidates.is_empty() {
            emit(step_id, "whisper-large-v3", "complete", 
                &format!("Found {} pre-detected profanity words with precise timestamps", bleep_candidates.len()));
        }

        // ── Build transcript text for LLM analysis ───────────
        let mut transcript_text = String::new();
        if let Some(segs) = transcript_data["segments"].as_array() {
            for seg in segs {
                let start = seg["start"].as_f64().unwrap_or(0.0);
                let text = seg["text"].as_str().unwrap_or("");
                if !text.is_empty() && text != "..." {
                    transcript_text.push_str(&format!("[{:.0}s] {}\n", start, text));
                }
            }
        }

        // ── LLM Compliance Analysis ──────────────────────────
        emit(step_id, "llama-3.3-70b", "processing", 
            "Analyzing transcript against CBFC, IMDA, and NMC broadcast regulations...");

        let prompt = format!(
            r#"You are a broadcast compliance officer. Analyze this transcript for regulatory issues.

TRANSCRIPT:
{}

CHECK AGAINST:
1. India CBFC: profanity, violence, tobacco/alcohol, religious sensitivity
2. Singapore IMDA: drug refs, gambling, content rating (G/PG/PG13/NC16/M18/R21)
3. UAE NMC: Islamic values, alcohol refs, profanity

For EACH violation, provide exact timecode_seconds from the transcript.
Required actions: bleep (audio), cut (remove), blur (visual), advisory (warning card)

Respond ONLY as JSON, no markdown:
{{"compliance_findings": [{{"timecode_seconds": N, "timecode_display": "M:SS", "content": "quoted text", "territory": "country", "regulation": "rule", "severity": "critical or warning", "action": "bleep or cut or blur or advisory"}}], "overall_rating": "rating string"}}"#,
            &transcript_text[..transcript_text.len().min(3000)]
        );

        let response = crate::pipeline::call_vllm_public(&prompt, 1500, 0.2).await?;

        emit(step_id, "llama-3.3-70b", "analyzing", "Parsing compliance findings...");

        // ── Parse LLM findings ───────────────────────────────
        let mut findings = Vec::new();
        let compliance_data: serde_json::Value = {
            let cleaned = response.trim()
                .trim_start_matches("```json")
                .trim_start_matches("```")
                .trim_end_matches("```")
                .trim();
            serde_json::from_str(cleaned).unwrap_or(json!({}))
        };

        if let Some(items) = compliance_data["compliance_findings"].as_array() {
            for item in items {
                let tc = item["timecode_seconds"].as_f64().unwrap_or(0.0);
                let content = item["content"].as_str().unwrap_or("Compliance issue");
                let territory = item["territory"].as_str().unwrap_or("Unknown");
                let severity = item["severity"].as_str().unwrap_or("warning");
                let action = item["action"].as_str().unwrap_or("review");
                let regulation = item["regulation"].as_str().unwrap_or("");
                let display = item["timecode_display"].as_str().unwrap_or("?");

                findings.push(Finding {
                    timecode_seconds: tc,
                    content: format!("[{}] \"{}\" — {} ({})", display, content, regulation, territory),
                    finding_type: "compliance".to_string(),
                    severity: severity.to_string(),
                    source: format!("compliance/{}", territory.to_lowercase().replace(' ', "_")),
                    suggested_action: Some(action.to_string()),
                });
            }
        }

        // ── Merge word-level bleep points from Whisper ───────
        // These have PRECISE timestamps (unlike LLM which gives approximate seconds)
        for bc in &bleep_candidates {
            let word = bc["word"].as_str().unwrap_or("");
            let start = bc["start"].as_f64().unwrap_or(0.0);
            let end = bc["end"].as_f64().unwrap_or(0.0);
            
            // Check if this word is already in LLM findings (avoid duplicates)
            let already_found = findings.iter().any(|f| {
                (f.timecode_seconds - start).abs() < 2.0 && 
                f.content.to_lowercase().contains(word.to_lowercase().trim_matches(|c: char| !c.is_alphanumeric()))
            });

            if !already_found {
                findings.push(Finding {
                    timecode_seconds: start,
                    content: format!("\"{}\" at {:.3}s-{:.3}s (word-level precision)", word, start, end),
                    finding_type: "compliance_bleep".to_string(),
                    severity: "warning".to_string(),
                    source: "whisper/word_timestamps".to_string(),
                    suggested_action: Some(format!("bleep {:.3}s-{:.3}s", start, end)),
                });
            }
        }

        let overall_rating = compliance_data["overall_rating"].as_str()
            .unwrap_or("Review required");

        emit(step_id, "llama-3.3-70b", "complete", 
            &format!("Compliance complete: {} findings, rating: {}", findings.len(), overall_rating));

        // Save compliance JSON
        let compliance_path = "/tmp/pipeline_compliance.json";
        let _ = tokio::fs::write(compliance_path, 
            serde_json::to_string_pretty(&json!({
                "findings": &findings,
                "bleep_candidates": &bleep_candidates,
                "overall_rating": overall_rating,
                "llm_response": &compliance_data,
            })).unwrap_or_default()
        ).await;

        let summary = format!(
            "Compliance check complete: {} findings across territories.\n\
             Rating: {}\n\
             Word-level bleep points: {} (precise timestamps from Whisper)\n\
             Artifact: {}",
            findings.len(), overall_rating, bleep_candidates.len(), compliance_path
        );

        Ok(SkillResult {
            skill_id: "compliance_check".into(),
            summary,
            output_type: OutputType::TimecodedNotes,
            data: json!({
                "territories": ["India CBFC", "Singapore IMDA", "UAE NMC"],
                "total_findings": findings.len(),
                "bleep_candidates": bleep_candidates.len(),
                "overall_rating": overall_rating,
            }),
            timecoded_findings: Some(findings),
            action_taken: None,
            artifacts: vec![compliance_path.to_string()],
        })
    }
}

// Cloud Run service using Functions Framework (Node.js) — CloudEvent handler
const { Storage } = require('@google-cloud/storage');
const { SpeechClient } = require('@google-cloud/speech').v2;

const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const functions = require('@google-cloud/functions-framework');
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;

const storage = new Storage();
const speech = new SpeechClient({ apiEndpoint: 'us-central1-speech.googleapis.com' });

const UPLOAD_BUCKET = must('BUCKET_UPLOADS');
const FLAC_TRANSCODES_BUCKET = must('FLAC_TRANSCODES_BUCKET');
const TXT_TRANSCRIPTS_BUCKET = must('TXT_TRANSCRIPTS_BUCKET');
const RECOGNIZER = must('RECOGNIZER');
const LANGUAGE_CODES = (process.env.LANGUAGE_CODES || 'en-US')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

// models: keep the SAME model for both paths for consistent quality
const SPEECH_MODEL_LONG = process.env.SPEECH_MODEL_LONG || 'latest_long';
const SPEECH_MODEL_SHORT = process.env.SPEECH_MODEL_SHORT || 'latest_short';

// threshold (seconds) under which we use sync recognize
const SYNC_MAX_SECONDS = Number(process.env.SYNC_MAX_SECONDS || '59'); // default 59s

functions.cloudEvent('onAudioUploaded', async (cloudevent) => {
  const { bucket, name: objectName, contentType, size } = cloudevent?.data || {};
  log('[cloudevent]', { type: cloudevent?.type, bucket, objectName, contentType, size });

  if (!bucket || !objectName) return;
  if (bucket !== UPLOAD_BUCKET) return;

  const base = path.basename(objectName).replace(/\.[^.]+$/, '');
  const tmpIn  = `/tmp/${base}.m4a`;
  const tmpOut = `/tmp/${base}.flac`;
  const flacKey = objectName.replace(/\.[^.]+$/, '.flac');

  try {
    await time('download.m4a', () =>
      storage.bucket(bucket).file(objectName).download({ destination: tmpIn })
    );

    // Probe duration (from the original file)
    const seconds = await probeDurationSec(tmpIn);
    log('[probe.duration]', { seconds });

    // Transcode to FLAC (mono) for consistent recognition (keep source sample rate) ★
    await time('ffmpeg.transcode->flac', () =>
      runFfmpeg([
        '-hide_banner','-loglevel','error',
        '-y','-i', tmpIn,
        '-ac','1',                 // mono
        '-sample_fmt','s16',       // 16-bit PCM (good for FLAC + ASR)
        '-vn',                     // no video
        '-map_metadata','-1',      // drop metadata; keeps payload minimal
        '-c:a','flac',
        '-compression_level','5',  // FLAC default; 5–8 is fine (trade CPU vs size)
        tmpOut
      ])
    );

    if (seconds <= SYNC_MAX_SECONDS) {
      // ---- SYNC PATH (short audio): call recognize, write .txt directly ----
      log('[stt.sync] starting recognize()');

      const audioB64 = (await fs.readFile(tmpOut)).toString('base64');

      // Make config identical to batch: same model, language codes, punctuation ★
      const config = {
        autoDecodingConfig: {},
        languageCodes: LANGUAGE_CODES,
        model: SPEECH_MODEL_SHORT,
        features: { enableAutomaticPunctuation: true }, // ★ punctuation helps WER & readability
        // You can also add phrase sets here if you used them in batch (v2 hint sets)
      };

      const [resp] = await time('speech.recognize', () =>
        speech.recognize({
          recognizer: RECOGNIZER,
          config,
          content: audioB64,
        })
      );

      const parts = [];
      for (const r of (resp?.results || [])) {
        const alt0 = (r.alternatives && r.alternatives[0]) || null;
        const t = alt0?.transcript?.trim();
        if (t) parts.push(t);
      }

      let text = parts.join('\n\n').trim();
      if (!text || text.length <= 0) {
        text = "[Empty transcript]";
      }

      const outName = `${base}.txt`;
      await time('write.txt', () =>
        storage.bucket(TXT_TRANSCRIPTS_BUCKET).file(outName).save(text, {
          resumable: false,
          contentType: 'text/plain; charset=utf-8',
          metadata: { cacheControl: 'no-cache' },
          ifGenerationMatch: 0, // create-only idempotency
        })
      );
      log('[stt.sync.done]', { txt: `gs://${TXT_TRANSCRIPTS_BUCKET}/${outName}`, bytes: text.length });

      // (Optional) archive FLAC:
      // await storage.bucket(FLAC_TRANSCODES_BUCKET).upload(tmpOut, { destination: flacKey, contentType: 'audio/flac' });

    } else {
      // ---- ASYNC PATH (long audio): upload FLAC, use batchRecognize to GCS JSON ----
      await time('upload.flac', () =>
        storage.bucket(FLAC_TRANSCODES_BUCKET).upload(tmpOut, { destination: flacKey, contentType: 'audio/flac' })
      );

      const gcsFlacUri = `gs://${FLAC_TRANSCODES_BUCKET}/${flacKey}`;
      const outUriPrefix = `gs://${TXT_TRANSCRIPTS_BUCKET}/`;

      // Config matches sync path ★
      const config = {
        autoDecodingConfig: {},
        languageCodes: LANGUAGE_CODES,
        model: SPEECH_MODEL_LONG,
        features: { enableAutomaticPunctuation: true }, // ★
      };

      try {
        const [operation] = await time('speech.batchRecognize.start', () =>
          speech.batchRecognize({
            recognizer: RECOGNIZER,
            files: [{ uri: gcsFlacUri }],
            config,
            recognitionOutputConfig: { gcsOutputConfig: { uri: outUriPrefix } }
          })
        );
        log('[speech.started]', { operationName: operation.name, outUriPrefix });
        // Eventarc will trigger your txtify service when JSON lands
      } catch (e) {
        console.error('[speech.error]', e?.details || e?.message || e);
        return; // avoid Eventarc retry storms
      }
    }
  } finally {
    await safeUnlink(tmpIn);
    await safeUnlink(tmpOut);
  }
});

function must(k){ const v=process.env[k]; if(!v) throw new Error(`Missing required env var ${k}`); return v; }

// --- helpers ---
function nowISO(){ return new Date().toISOString(); }
function log(msg, obj){ obj ? console.log(`${nowISO()} ${msg}`, obj) : console.log(`${nowISO()} ${msg}`); }
async function time(label, fn){
  const t0 = Date.now(); log(`[t0] ${label}`);
  const out = await fn();
  const ms = Date.now() - t0; log(`[t1] ${label} (+${ms} ms)`);
  return out;
}

// Run ffmpeg and collect stderr for errors
function runFfmpeg(args){
  return new Promise((res, rej) => {
    const p = spawn(ffmpegPath, args, { stdio:['ignore','pipe','pipe'] });
    let err=''; p.stderr.on('data', d => { err += d; });
    p.on('close', c => c === 0 ? res() : rej(new Error(`ffmpeg exit ${c}: ${err}`)));
  });
}

// Quick duration probe by parsing "Duration: hh:mm:ss.xx" from ffmpeg -i
async function probeDurationSec(inputPath){
  return new Promise((resolve) => {
    const p = spawn(ffmpegPath, ['-i', inputPath], { stdio:['ignore','pipe','pipe'] });
    let stderr = '';
    p.stderr.on('data', d => { stderr += d.toString(); });
    p.on('close', () => {
      const m = stderr.match(/Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/);
      if (!m) return resolve(NaN);
      const h = Number(m[1]), min = Number(m[2]), s = Number(m[3]);
      resolve(h*3600 + min*60 + s);
    });
    setTimeout(() => { try{ p.kill('SIGKILL'); }catch{} }, 5000);
  });
}

async function safeUnlink(p){ try{ await fs.unlink(p);}catch{} }

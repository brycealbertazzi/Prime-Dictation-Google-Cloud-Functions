// transcode-transcribe/index.js
// Cloud Functions (Gen2) — GCS finalize trigger
// Flow: raw/<name>.m4a → /tmp → ffmpeg → flac/<name>.flac → Speech v2 (BatchRecognize)
// Speech writes JSON to transcribed-files/… ; a separate txtify function will prettify to .txt

const { Storage } = require('@google-cloud/storage');
const { SpeechClient } = require('@google-cloud/speech').v2;
const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');

const storage = new Storage();
const speech = new SpeechClient();

// -------- Env vars (set at deploy) --------
// BUCKET_UPLOADS        = prime-dictation-audio-files
// RECOGNIZER            = projects/<PROJECT_ID>/locations/us-central1/recognizers/prime-default
// TRANSCRIPTS_PREFIX    = transcribed-files/      (default)
// FLAC_PREFIX           = flac/                   (default)
// LANGUAGE_CODES        = en-US                   (comma-separated; optional)
// SPEECH_MODEL          = long                    (optional)
// ------------------------------------------

const UPLOAD_BUCKET      = must('BUCKET_UPLOADS');
const RECOGNIZER         = must('RECOGNIZER');
const TRANSCRIPTS_PREFIX = process.env.TRANSCRIPTS_PREFIX || 'transcribed-files/';
const FLAC_PREFIX        = process.env.FLAC_PREFIX || 'flac/';
const LANGUAGE_CODES     = (process.env.LANGUAGE_CODES || 'en-US').split(',').map(s => s.trim()).filter(Boolean);
const SPEECH_MODEL       = process.env.SPEECH_MODEL || 'long';

exports.onAudioUploaded = async (event /*, context */) => {
  const { bucket, name: objectName, contentType, size } = event || {};
  if (!bucket || !objectName) return;
  if (bucket !== UPLOAD_BUCKET) return;
  if (!objectName.startsWith('raw/')) return; // only handle uploads under raw/

  const base = path.basename(objectName).replace(/\.[^.]+$/, '');   // "foo"
  const tmpIn  = `/tmp/${base}.m4a`;
  const tmpOut = `/tmp/${base}.flac`;
  const flacKey = objectName.replace(/^raw\//, FLAC_PREFIX).replace(/\.[^.]+$/, '.flac');

  console.log('[start]', { bucket, objectName, contentType, size, flacKey });

  try {
    // 1) Download input to /tmp
    await storage.bucket(bucket).file(objectName).download({ destination: tmpIn });
    console.log('[downloaded]', tmpIn);

    // 2) Transcode to FLAC (mono, 16kHz, s16)
    await runFfmpeg([
      '-y',
      '-i', tmpIn,
      '-ac', '1',           // mono
      '-ar', '16000',       // 16 kHz
      '-sample_fmt', 's16', // 16-bit PCM (lossless inside FLAC)
      '-vn',
      '-c:a', 'flac',
      tmpOut
    ]);
    console.log('[transcoded]', tmpOut);

    // 3) Upload FLAC
    await storage.bucket(bucket).upload(tmpOut, {
      destination: flacKey,
      contentType: 'audio/flac'
    });
    console.log('[uploaded flac]', flacKey);

    // 4) Kick off Speech v2 BatchRecognize (async; do NOT wait)
    const gcsFlacUri = `gs://${bucket}/${flacKey}`;
    const outUriPrefix = `gs://${bucket}/${TRANSCRIPTS_PREFIX}`;

    const request = {
      recognizer: RECOGNIZER,
      files: [{ uri: gcsFlacUri }],
      config: {
        autoDecodingConfig: {},           // FLAC handled automatically
        languageCodes: LANGUAGE_CODES,
        model: SPEECH_MODEL
      },
      outputConfig: {
        gcsOutputConfig: { uri: outUriPrefix }
      }
    };

    console.log('[speech.batchRecognize] ->', { input: gcsFlacUri, output: outUriPrefix, recognizer: RECOGNIZER });
    const [operation] = await speech.batchRecognize(request);
    console.log('[speech.started]', { operationName: operation.name });

  } catch (err) {
    console.error('[error]', err?.stack || err?.message || err);
    throw err; // allow retries on transient errors
  } finally {
    await safeUnlink(tmpIn);
    await safeUnlink(tmpOut);
    console.log('[done]', { objectName });
  }
};

// ---------- helpers ----------
function must(k) {
  const v = process.env[k];
  if (!v) throw new Error(`Missing required env var ${k}`);
  return v;
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn('/usr/bin/ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let err = '';
    p.stderr.on('data', d => { err += d.toString(); });
    p.on('close', code => code === 0 ? resolve() : reject(new Error(`ffmpeg exit ${code}: ${err}`)));
  });
}

async function safeUnlink(p) {
  try { await fs.unlink(p); } catch {}
}

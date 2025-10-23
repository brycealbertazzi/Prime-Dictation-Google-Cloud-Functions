// Cloud Run service using Functions Framework (Node.js) — CloudEvent handler
const { Storage } = require('@google-cloud/storage');
const { SpeechClient } = require('@google-cloud/speech').v2;
const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const functions = require('@google-cloud/functions-framework');
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;

const storage = new Storage();
const speech = new SpeechClient();

const UPLOAD_BUCKET = must('BUCKET_UPLOADS');
const FLAC_TRANSCODES_BUCKET = must('FLAC_TRANSCODES_BUCKET');
const TXT_TRANSCRIPTS_BUCKET = must('TXT_TRANSCRIPTS_BUCKET');
const RECOGNIZER = must('RECOGNIZER');
const LANGUAGE_CODES = (process.env.LANGUAGE_CODES || 'en-US').split(',').map(s => s.trim()).filter(Boolean);
const SPEECH_MODEL = process.env.SPEECH_MODEL || 'long';

functions.cloudEvent('onAudioUploaded', async (cloudevent) => {
  // Storage (direct) event payload is under cloudevent.data
  const { bucket, name: objectName, contentType, size } = cloudevent?.data || {};
  console.log('[cloudevent]', { type: cloudevent?.type, bucket, objectName, contentType, size });

  if (!bucket || !objectName) return;
  if (bucket !== UPLOAD_BUCKET) return;

  const base = path.basename(objectName).replace(/\.[^.]+$/, '');
  const tmpIn  = `/tmp/${base}.m4a`;
  const tmpOut = `/tmp/${base}.flac`;
  const flacKey = objectName.replace(/\.[^.]+$/, '.flac');

  try {
    await storage.bucket(bucket).file(objectName).download({ destination: tmpIn });
    await runFfmpeg(['-y','-i', tmpIn,'-ac','1','-ar','16000','-sample_fmt','s16','-vn','-c:a','flac', tmpOut]);
    await storage.bucket(FLAC_TRANSCODES_BUCKET).upload(tmpOut, { destination: flacKey, contentType: 'audio/flac' });

    const gcsFlacUri = `gs://${FLAC_TRANSCODES_BUCKET}/${flacKey}`;
    const outUriPrefix = `gs://${TXT_TRANSCRIPTS_BUCKET}`;

    const [operation] = await speech.batchRecognize({
      recognizer: RECOGNIZER,
      files: [{ uri: gcsFlacUri }],
      config: { autoDecodingConfig: {}, languageCodes: LANGUAGE_CODES, model: SPEECH_MODEL },
      recognitionOutputConfig: { gcsOutputConfig: { uri: outUriPrefix } },
    });
    console.log('[speech.started]', { operationName: operation.name, input: gcsFlacUri, output: outUriPrefix });
  } finally {
    await safeUnlink(tmpIn);
    await safeUnlink(tmpOut);
  }
});

function must(k){ const v=process.env[k]; if(!v) throw new Error(`Missing required env var ${k}`); return v; }
function runFfmpeg(args){ return new Promise((res,rej)=>{ const p=spawn(ffmpegPath,args,{stdio:['ignore','pipe','pipe']}); let err=''; p.stderr.on('data',d=>{err+=d}); p.on('close',c=>c===0?res():rej(new Error(`ffmpeg exit ${c}: ${err}`)));});}
async function safeUnlink(p){ try{ await fs.unlink(p);}catch{} }

// txtify/index.js
// Cloud Functions (Gen2) — GCS finalize trigger on transcribed-files/*.json
// Reads Speech v2 JSON, prettifies into plain text, writes <same>.txt (overwrites by default)

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

// Env
const BUCKET             = process.env.BUCKET_UPLOADS || 'prime-dictation-audio-files';
const TRANSCRIPTS_PREFIX = process.env.TRANSCRIPTS_PREFIX || 'transcribed-files/';
const CREATE_ONLY        = (process.env.CREATE_ONLY || 'false').toLowerCase() === 'true';

exports.txtify = async (event /*, context */) => {
  const { bucket, name: objectName, contentType } = event || {};
  if (bucket !== BUCKET) return;
  if (!objectName?.startsWith(TRANSCRIPTS_PREFIX)) return;
  if (!/\.json$/i.test(objectName)) return;

  console.log('[txtify] new JSON:', { objectName, contentType });

  // 1) Download and parse Speech v2 JSON
  const [raw] = await storage.bucket(bucket).file(objectName).download();
  const payload = JSON.parse(String(raw));

  // 2) Extract transcripts: results[].alternatives[].transcript
  const lines = [];
  for (const r of payload.results ?? []) {
    for (const a of r.alternatives ?? []) {
      if (a.transcript) lines.push(a.transcript);
    }
  }

  // 3) Light prettify: trim/collapse whitespace; join with newlines
  const pretty = lines
    .map(s => s.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .join('\n');

  // 4) Write .txt next to the JSON (overwrite by default)
  const txtName = objectName.replace(/\.json$/i, '.txt');
  const file = storage.bucket(bucket).file(txtName);

  const saveOpts = {
    contentType: 'text/plain; charset=utf-8',
    ...(CREATE_ONLY ? { preconditionOpts: { ifGenerationMatch: 0 } } : {})
  };

  await file.save(pretty, saveOpts);
  console.log('[txtify] wrote:', txtName, { bytes: Buffer.byteLength(pretty, 'utf8'), overwrite: !CREATE_ONLY });
};

// Cloud Run service using Functions Framework (Node.js) — CloudEvent + HTTP
// CE path: finalize of JSON in TXT bucket -> writes <base>.txt
// HTTP path: GET /sign?name=<objectName> -> returns a v4 signed URL to download the .txt

const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const path = require('path');

const storage = new Storage();

const TXT_TRANSCRIPTS_BUCKET = must('TXT_TRANSCRIPTS_BUCKET'); // prime-dictation-txt-files

// ---- CloudEvent: turn Speech v2 JSON into .txt ----
functions.cloudEvent('onAudioTranscribed', async (ce) => {
  const d = ce?.data || {};
  const bucket = d.bucket;
  const name   = d.name;

  console.log('[ce]', { type: ce?.type, bucket, name });
  if (!bucket || !name) return;
  if (bucket !== TXT_TRANSCRIPTS_BUCKET) { console.log('[skip] different bucket'); return; }
  if (!name.toLowerCase().endsWith('.json')) { console.log('[skip] not json'); return; }

  // Read JSON
  const file = storage.bucket(bucket).file(name);
  const [buf] = await file.download();
  let json;
  try { json = JSON.parse(buf.toString('utf8')); }
  catch (e) { console.error('[parse-error]', e?.message); return; }

  // Extract text from Speech v2 result(s)
  // Handles {results:[{alternatives:[{transcript,confidence}], ...}]}
  const results = Array.isArray(json?.results) ? json.results : [];
  const pieces = [];
  for (const r of results) {
    const alt = Array.isArray(r?.alternatives) ? r.alternatives[0] : null;
    if (alt?.transcript) pieces.push(alt.transcript.trim());
  }
  const text = pieces.join('\n\n').trim();
  if (!text) { console.log('[skip] empty transcript'); return; }

  // Derive output name: replace only trailing ".json" with ".txt"
  const base = name.replace(/\.json$/i, '');
  const outName = `${base}.txt`;

  await storage.bucket(bucket).file(outName).save(text, {
    resumable: false,
    contentType: 'text/plain; charset=utf-8',
    metadata: { cacheControl: 'no-cache' }
  });

  console.log('[wrote]', `gs://${bucket}/${outName}`, `${text.length} bytes`);
});

// ---- HTTP: mint a V4 signed URL so your iOS app can fetch the .txt ----
// Call: GET /sign?name=<objectName>
functions.http('sign', async (req, res) => {
  try {
    const { name } = req.query || {};
    if (!name || typeof name !== 'string') {
      res.status(400).json({ error: 'missing ?name=' }); return;
    }
    const [url] = await storage
      .bucket(TXT_TRANSCRIPTS_BUCKET)
      .file(name)
      .getSignedUrl({
        version: 'v4',
        action: 'read',
        expires: Date.now() + 15 * 60 * 1000, // 15 min
        responseDisposition: `attachment; filename="${path.basename(name)}"`
      });
    res.json({ url });
  } catch (e) {
    console.error('[sign.error]', e?.message || e);
    res.status(500).json({ error: 'signing-failed' });
  }
});

function must(k){ const v=process.env[k]; if(!v) throw new Error(`Missing env ${k}`); return v; }

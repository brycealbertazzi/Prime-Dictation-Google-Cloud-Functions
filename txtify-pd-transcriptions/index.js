// Cloud Run + Functions Framework
// - CloudEvent: finalize JSON in TXT bucket -> writes <base>.txt (idempotent)
// - HTTP: GET /sign?name=<objectName> -> V4 signed URL (15 min)
const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const path = require('path');

const storage = new Storage();

// ---- helpers ----
const isSafeName = (n) => typeof n === 'string' && !n.startsWith('/') && !n.includes('..');

functions.cloudEvent('onAudioTranscribed', async (ce) => {
  try {
    const TXT_BUCKET = process.env.TXT_TRANSCRIPTS_BUCKET || process.env.TXT_BUCKET;
    if (!TXT_BUCKET) { console.error('Missing TXT_TRANSCRIPTS_BUCKET/TXT_BUCKET'); return; }

    const d = ce?.data || {};
    const bucket = d.bucket;
    const name   = d.name;

    console.log('[ce]', { type: ce?.type, bucket, name });
    if (!bucket || !name) return;
    if (bucket !== TXT_BUCKET) { console.log('[skip] different bucket'); return; }
    if (!isSafeName(name)) { console.log('[skip] unsafe name'); return; }
    if (!name.toLowerCase().endsWith('.json')) { console.log('[skip] not json'); return; }

    const base    = name.replace(/\.json$/i, '');
    const outName = `${base}.txt`;

    // Idempotency: if .txt already exists, bail (helps with retries)
    const outFile = storage.bucket(bucket).file(outName);
    const [exists] = await outFile.exists();
    if (exists) { console.log('[skip] txt already exists', outName); return; }

    // Read JSON
    const file = storage.bucket(bucket).file(name);
    const [buf] = await file.download();
    let json;
    try { json = JSON.parse(buf.toString('utf8')); }
    catch (e) { console.error('[parse-error]', e?.message); return; }

    // Extract transcript text
    // Speech v2 typical shape: { results: [{ alternatives: [{ transcript, confidence }], ... }] }
    const results = Array.isArray(json?.results) ? json.results : [];
    const parts = [];
    for (const r of results) {
      const alt0 = Array.isArray(r?.alternatives) ? r.alternatives[0] : null;
      const t = alt0?.transcript?.trim();
      if (t) parts.push(t);
    }
    const text = parts.join('\n\n').trim();
    if (!text) { console.log('[skip] empty transcript'); return; }

    await outFile.save(text, {
      resumable: false,
      contentType: 'text/plain; charset=utf-8',
      metadata: { cacheControl: 'no-cache' }
    });

    console.log('[wrote]', `gs://${bucket}/${outName}`, `${text.length} bytes`);
  } catch (err) {
    console.error('[unhandled]', err?.stack || err);
    // Do not rethrow; let Eventarc consider it handled to avoid retry storms.
  }
});

// HTTP: GET /sign?name=<objectName> -> V4 signed URL to download .txt
functions.http('sign', async (req, res) => {
  try {
    const TXT_BUCKET = process.env.TXT_TRANSCRIPTS_BUCKET || process.env.TXT_BUCKET;
    if (!TXT_BUCKET) return res.status(500).json({ error: 'missing TXT bucket env' });

    const { name } = req.query || {};
    if (!isSafeName(name)) return res.status(400).json({ error: 'bad name' });

    const [url] = await storage.bucket(TXT_BUCKET).file(String(name)).getSignedUrl({
      version: 'v4',
      action: 'read',
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${path.basename(String(name))}"`
    });

    res.json({ url });
  } catch (e) {
    console.error('[sign.error]', e?.message || e);
    res.status(500).json({ error: 'signing-failed' });
  }
});

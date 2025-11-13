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

    // NEW: derive the exact TXT filename you want
    const outName = jsonToTxtName(name);
    const outFile = storage.bucket(bucket).file(outName);

    // Read JSON
    const file = storage.bucket(bucket).file(name);
    const [buf] = await file.download();

    let json;
    try { json = JSON.parse(buf.toString('utf8')); }
    catch (e) { console.error('[parse-error]', e?.message); return; }

    // Extract transcript text
    const results = Array.isArray(json?.results) ? json.results : [];
    const parts = [];
    for (const r of results) {
      const alt0 = Array.isArray(r?.alternatives) ? r.alternatives[0] : null;
      const t = alt0?.transcript?.trim();
      if (t) parts.push(t);
    }
    
    let text = parts.join('\n\n').trim();
    if (!text || text.length <= 0) {
      text = "[Empty transcript]";
    }

    outFile.save(text, {
      resumable: false,
      contentType: 'text/plain; charset=utf-8',
      metadata: { cacheControl: 'no-cache' },
      ifGenerationMatch: 0, // create-only; avoids an extra exists() call
    });

  } catch (err) {
    console.error('[unhandled]', err?.stack || err);
  }
});

// Map JSON object name -> desired TXT name (strip STT suffixes like _transcript_<uuid>)
function jsonToTxtName(jsonName) {
  // remove leading/trailing spaces just in case
  const n = String(jsonName).trim();

  const cleanedBase = n
    .replace(/(_transcript.*)?\.json$/i, '')   // strips "_transcript..." if present
    .replace(/(_result-\d+)?$/i, '');          // optional: strips "_result-<n>" if present

  return `${cleanedBase}.txt`;
}

// HTTP: GET /sign?name=<objectName> -> V4 signed URL to download .txt
functions.http('sign', async (req, res) => {
  try {
    const TXT_BUCKET = process.env.TXT_TRANSCRIPTS_BUCKET || process.env.TXT_BUCKET;
    if (!TXT_BUCKET) return res.status(500).json({ error: 'missing TXT bucket env' });

    const { name } = req.query || {};
    if (!isSafeName(name)) return res.status(400).json({ error: 'bad name' });

    const file = storage.bucket(TXT_BUCKET).file(String(name));
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: 'not-found' });

    const [url] = await file.getSignedUrl({
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

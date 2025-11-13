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

    log('[ce]', { type: ce?.type, bucket, name });
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
    log('INFO', 'Incoming transcript', { length: text.length, sample: text.slice(0, 200) });
    if (!text || text.length <= 0) {
      text = "[Empty transcript]";
    } else {
      text = normalizeTranscript(text)
    }

    /**
     * Normalize a transcript string:
     * - Condenses all newlines to spaces
     * - Converts spoken punctuation to symbols (., , ? ! : ;)
     *   * Skips conversion if preceded by the word "literal"
     *   * Attaches punctuation to the previous token (no extra space)
     * - Cleans up spaces around punctuation
    */
    function normalizeTranscript(text) {
      text = String(text || '').trim();
      if (!text) return '[Empty transcript]';

      // 1) Condense any kind of newline to a single space, then collapse spaces
      text = text
        .replace(/\s*(?:\r\n|\r|\n|\u2028|\u2029)+\s*/g, ' ')
        .replace(/\s{2,}/g, ' ')
        .trim();

      // 2) Spoken punctuation pass (idempotent + "literal" escape)
      const toks = text.split(/\s+/);
      const out = [];
      const lower = (s) => (s || '').toLowerCase();
      const peek = (i) => (i < toks.length ? toks[i] : null);

      const attach = (symbol) => {
        if (out.length) {
          if (!out[out.length - 1].endsWith(symbol)) {
            out[out.length - 1] = out[out.length - 1] + symbol;
          }
        } else {
          out.push(symbol);
        }
      };

      const dropLiteralAndKeep = (word) => {
        out.pop();        // drop "literal"
        out.push(word);   // keep the spoken word as-is
      };

      for (let i = 0; i < toks.length; i++) {
        const cur = toks[i];
        const curL = lower(cur);
        const prevWord = out.length ? out[out.length - 1] : '';
        const prevIsLiteral = lower(prevWord) === 'literal';
        const next = peek(i + 1);
        const nextL = lower(next);

        // Two-word phrases
        if (curL === 'question' && nextL === 'mark') {
          if (prevIsLiteral) {
            dropLiteralAndKeep(cur);
            i += 1; out.push(next);
          } else {
            attach('?'); i += 1;
          }
          continue;
        }
        if (curL === 'exclamation' && (nextL === 'mark' || nextL === 'point')) {
          if (prevIsLiteral) {
            dropLiteralAndKeep(cur);
            i += 1; out.push(next);
          } else {
            attach('!'); i += 1;
          }
          continue;
        }

        // One-word punctuation
        if (['period', 'comma', 'colon', 'semicolon'].includes(curL)) {
          if (prevIsLiteral) {
            dropLiteralAndKeep(cur);
          } else {
            const map = { period: '.', comma: ',', colon: ':', semicolon: ';' };
            attach(map[curL]);
          }
          continue;
        }

        // Default
        out.push(cur);
      }

      // 3) Rebuild and tidy spacing around punctuation
      let normalized = out.join(' ');
      normalized = normalized.replace(/\s+([.,!?;:])/g, '$1');      // no space before
      normalized = normalized.replace(/([.,!?;:])([^\s"'\)\]}])/g, '$1 $2'); // space after
      normalized = normalized.replace(/\s{2,}/g, ' ').trim();

      return normalized || '[Empty transcript]';
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

function nowISO(){ return new Date().toISOString(); }
function log(msg, obj){ obj ? console.log(`${nowISO()} ${msg}`, obj) : console.log(`${nowISO()} ${msg}`); }

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

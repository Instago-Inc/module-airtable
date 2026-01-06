// airtable@1.0.0 — minimal Airtable API helper
// API:
// - configure({ apiKey })
// - query({ baseId, table, filterByFormula?, maxRecords? })
// - upsert({ baseId, table, records: [{ id?|filter?, fields }], typecast? })

(function(){
  const httpx = require('http@1.0.0');
  const qs = require('qs@1.0.0');
  const log = require('log@1.0.0').create('airtable');

  const cfg = { key: null, api: 'https://api.airtable.com/v0' };
  const MAX_BATCH = 10;

  function configure(opts){
    if (!opts || typeof opts !== 'object') return;
    if (opts.apiKey) cfg.key = String(opts.apiKey);
    if (opts.api) cfg.api = String(opts.api);
  }
  function token(){ return cfg.key || sys.env.get('airtable.key') || null; }
  function hdr(){ const t = token(); if (!t) return null; return { 'Authorization': 'Bearer ' + t, 'Content-Type': 'application/json' }; }
  function apiBase(){ return cfg.api || sys.env.get('airtable.api') || 'https://api.airtable.com/v0'; }
  function buildUrl(baseId, table, query){
    const q = query && Object.keys(query).length ? ('?' + qs.encode(query)) : '';
    return apiBase() + '/' + encodeURIComponent(baseId) + '/' + encodeURIComponent(table) + q;
  }
  function parseErr(body, status){
    try {
      if (body && body.error) return body.error.message || body.error.type || 'error';
    } catch {}
    return 'status ' + (status || 'unknown');
  }

  async function query({ baseId, table, filterByFormula, maxRecords, pageSize, retry=true, timeoutMs }){
    try {
      const h = hdr(); if (!h) return { ok:false, error:'airtable: missing apiKey' };
      const out = [];
      let offset;
      let remaining = maxRecords ? Math.max(0, Number(maxRecords)) : null;
      do {
        const q = {};
        if (filterByFormula) q.filterByFormula = filterByFormula;
        if (pageSize) q.pageSize = Math.min(Math.max(1, Number(pageSize)), MAX_BATCH * 10);
        if (remaining != null) q.maxRecords = Math.min(remaining || MAX_BATCH, MAX_BATCH * 10);
        if (offset) q.offset = offset;
        const url = buildUrl(baseId, table, q);
        const r = await httpx.json({ url, method:'GET', headers: h, retry, timeoutMs });
        const body = r && (r.json || null);
        const status = r && r.status;
        if (!body || (status && status >= 400)) {
          return { ok:false, status, error:'airtable: query failed - ' + parseErr(body, status), body };
        }
        if (Array.isArray(body.records)) out.push.apply(out, body.records);
        offset = body.offset;
        if (remaining != null) remaining -= (Array.isArray(body.records) ? body.records.length : 0);
      } while (offset && (remaining == null || remaining > 0));
      return { ok:true, data: { records: out, offset } };
    } catch (e){ log.error('query:error', e && (e.message||e)); return { ok:false, error: (e && (e.message||String(e))) || 'unknown' }; }
  }

  // Upsert strategy:
  // - If record has id -> PATCH that record
  // - Otherwise -> create via POST
  async function upsert({ baseId, table, records, typecast, timeoutMs }){
    try {
      const h = hdr(); if (!h) return { ok:false, error:'airtable: missing apiKey' };
      const create = []; const patch = [];
      (Array.isArray(records)?records:[]).forEach(r => {
        if (r && r.id) patch.push({ id: String(r.id), fields: r.fields || {} });
        else if (r && r.fields) create.push({ fields: r.fields });
      });
      const responses = { patched: [], created: [] };
      async function sendBatch(method, items){
        const url = buildUrl(baseId, table);
        const resp = await httpx.json({ url, method, headers: h, bodyObj: { records: items, typecast: !!typecast }, retry: false, timeoutMs });
        const status = resp && resp.status;
        const body = resp && (resp.json || null);
        if (!body || (status && status >= 400) || (body.records && !Array.isArray(body.records))) {
          return { ok:false, status, error:'airtable: upsert failed - ' + parseErr(body, status), body };
        }
        return { ok:true, body };
      }
      for (let i=0;i<patch.length;i+=MAX_BATCH){
        const r = await sendBatch('PATCH', patch.slice(i, i+MAX_BATCH));
        if (!r.ok) return r;
        if (r.body && Array.isArray(r.body.records)) responses.patched.push.apply(responses.patched, r.body.records);
      }
      for (let i=0;i<create.length;i+=MAX_BATCH){
        const r = await sendBatch('POST', create.slice(i, i+MAX_BATCH));
        if (!r.ok) return r;
        if (r.body && Array.isArray(r.body.records)) responses.created.push.apply(responses.created, r.body.records);
      }
      return { ok:true, data: responses };
    } catch (e){ log.error('upsert:error', e && (e.message||e)); return { ok:false, error: (e && (e.message||String(e))) || 'unknown' }; }
  }

  module.exports = { configure, query, upsert };
})();

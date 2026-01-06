# module/airtable - V8 module

Pull and update Airtable rows without dealing with the raw API. Provide your base/table plus credentials and use a small set of calls to query or upsert records.

Use it when you want to sync or enrich Airtable data from a workflow while keeping steps short. Learn more at https://instago.ai.

## Technical reference

### Exports
- `configure({ apiKey, api })` -> set API key and base URL (default `https://api.airtable.com/v0`).
- `query({ baseId, table, filterByFormula?, maxRecords?, pageSize?, retry?, timeoutMs? })` -> GET records.
- `upsert({ baseId, table, records: [{ id?|fields }], typecast?, timeoutMs? })` -> PATCH existing records with `id`, POST new ones without `id`.

### Configuration
Set these required `env` keys:
- `airtable.key` (API key)
- `airtable.api` (base URL, usually `https://api.airtable.com/v0`)

### Example
```js
const airtable = require('airtable@1.0.0');

airtable.configure({ apiKey: env.airtable.key });

// Query
const res = await airtable.query({
  baseId: 'app123',
  table: 'People',
  filterByFormula: "FIND('active', {status})"
});

// Upsert
await airtable.upsert({
  baseId: 'app123',
  table: 'People',
  records: [
    { fields: { Name: 'Jane', Status: 'active' } },
    { id: 'rec456', fields: { Status: 'inactive' } }
  ],
  typecast: true
});
```

## Notes
- Returns `{ ok, data|error }` and logs via `log@1.0.0`.

# module/airtable - V8 module

Keep Airtable data in sync with lightweight steps that are easy to reason about. This helper stays focused on the most common read and write flows so automation can move fast without extra ceremony.

It is designed for workflow-friendly scripts running on Instago's V8 runtime. Learn more at https://instago.ai.

## Technical reference

### Exports
- `configure({ apiKey, api })` -> set the API key and optional base URL override (default: `https://api.airtable.com/v0`).
- `query({ baseId, table, filterByFormula?, maxRecords?, pageSize?, retry?, timeoutMs? })` -> returns `{ ok, data: { records, offset }, error, status?, body? }`.
- `upsert({ baseId, table, records: [{ id?, fields }], typecast?, timeoutMs? })` -> returns `{ ok, data: { patched, created }, error, status?, body? }`.

### Usage
- Call `configure` once if you want to override credentials or the API base URL.
- `query` paginates until `offset` is empty or `maxRecords` is reached.
- `upsert` patches records with `id` and creates the rest in batches of 10.

### Configuration
Provide credentials via `configure` or set these required `env` keys:
- `airtable.key`
- `airtable.api`

### Examples
```js
const airtable = require('airtable@latest');

airtable.configure({ apiKey: env['airtable.key'] });

const list = await airtable.query({
  baseId: 'app123',
  table: 'People',
  filterByFormula: "FIND('active', {status})",
  maxRecords: 50
});

const saved = await airtable.upsert({
  baseId: 'app123',
  table: 'People',
  records: [
    { fields: { Name: 'Jane', Status: 'active' } },
    { id: 'rec456', fields: { Status: 'inactive' } }
  ],
  typecast: true
});
```

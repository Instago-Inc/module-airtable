# module/airtable - V8 module

Keep Airtable updates tidy and repeatable without wiring a full client. This module focuses on the calls most workflows need for syncing and enriching records.

It is a lightweight helper meant for short, readable steps. Learn more at https://instago.ai.

## Technical reference

### Exports
- `configure({ apiKey, api })` -> set the API key and base URL override (default base URL: `https://api.airtable.com/v0`).
- `query({ baseId, table, filterByFormula?, maxRecords?, pageSize?, retry?, timeoutMs? })` -> returns `{ ok, data: { records, offset }, error, status?, body? }`.
- `upsert({ baseId, table, records: [{ id?, fields }], typecast?, timeoutMs? })` -> returns `{ ok, data: { patched, created }, error, status?, body? }`.

### Usage
- Call `configure` if you want to override the API key or base URL for the process.
- `query` paginates until there is no `offset` or `maxRecords` is reached.
- `upsert` updates records with `id` via PATCH and creates the rest via POST (batches of 10).

### Configuration
Provide credentials via `configure` or set these required `env` keys:
- `airtable.key` (API key)
- `airtable.api` (base URL, usually `https://api.airtable.com/v0`)

### Examples
```js
const airtable = require('airtable@latest');

airtable.configure({ apiKey: env.airtable.key });

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

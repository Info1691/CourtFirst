# CourtFirst

Proof-of-concept pipeline that:
1. Reads `data/cases.csv`
2. Scrapes public case pages (or uses `local_text`)
3. Builds breach candidates by scanning text for breach-like phrases
4. Exports `out/breaches.json` in **Breach-ui** schema with provenance

## Data

`data/cases.csv` must have these columns:

- `case_id` (required)
- `source_url` (required unless `local_text` provided)
- `title` (optional, defaults to `case_id`)
- `local_text` (optional: if supplied, avoids scraping)
- `jurisdiction` (optional)

Example:

```csv
case_id,title,source_url,local_text,jurisdiction
JRC_2019_037,"Zhang v DBS Trustee",https://www.jerseylaw.je/judgments/unreported/pages/uid9a0fa2c3c9f8.aspx,,Jersey

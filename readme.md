# CourtFirst

Court-first extraction of breach-related findings straight from judgments (e.g., Jersey Law / BAILII).
Outputs:
- `out/breach_candidates.json` – mined phrases + paragraph-level provenance
- `out/breaches.json` – Breach-ui ready, grouped by tag, **no hard-coded terms**

Run via GitHub Actions (see `.github/workflows/scrape.yml`).

## How it works
1. Read `data/cases.csv` URLs (you control which cases to include).
2. Fetch the judgment HTML (polite, cached).
3. Parse paragraphs, focus on *outcome zones* (Held/Conclusion/Order) and sentences with “held/found/ordered/liable”.
4. Mine what the **court says** (e.g., “breach of duty”, “failure to account”), skipping explicit negatives (e.g., “no breach”).
5. Emit JSON with provenance (case id, paragraph id, snippet).

## Quick start
- Add more rows in `data/cases.csv`.
- Run the **Scrape judgments → breaches.json** workflow.
- Download the **artifacts** from Actions to review. (Optional) Enable the PR step to update `Breach-ui`.

## Legal
Respect robots.txt and terms of use of target sites. Use official/public sources that allow reuse.

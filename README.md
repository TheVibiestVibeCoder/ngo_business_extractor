# NGO-Business Anfragen Scraper

Scrapes parliamentary inquiries (*Anfragen*) related to NGO topics from the Austrian Parliament API, downloads the associated PDFs, and exports everything to CSV or JSON.

---

## Quick Start

```bash
pip install requests beautifulsoup4 lxml

python scraper.py --range 12months
python scraper.py --last 10
python scraper.py --from 01.01.2025 --to 31.03.2025
```

---

## How It Works

1. **Fetches** all Anfragen (type `J`) from the Parliament API for the relevant legislative periods
2. **Filters** rows by NGO keywords (`ngo`, `ngo-business`, `non-governmental`, …)
3. **Scrapes** each detail page to find the "Anfrage (gescanntes Original)" PDF link (3 fallback strategies)
4. **Downloads** PDFs and reports `OK` / `SKIP` / `FAILED` per inquiry
5. **Exports** all data to `output/anfragen_<range>_<timestamp>.csv` (and/or `.json`)

---

## Scope — Time & Range

Exactly one of `--range`, `--last`, or `--from` is required.

| Flag | Example | Description |
|---|---|---|
| `--range RANGE` | `--range 12months` | Predefined time window (see options below) |
| `--last N` | `--last 25` | The N most recent NGO inquiries, newest first |
| `--from DD.MM.YYYY` | `--from 01.01.2025` | Explicit start date |
| `--to DD.MM.YYYY` | `--to 31.03.2025` | Explicit end date (combine with `--from` or `--range`) |

**`--range` options:**
`1week` · `1month` · `3months` · `6months` · `12months` · `1year` · `2years` · `3years` · `5years` · `10years`

```bash
python scraper.py --range 6months
python scraper.py --from 01.01.2025 --to 30.06.2025
python scraper.py --last 50
```

---

## Filtering

### By keyword

| Flag | Example | Description |
|---|---|---|
| `--keyword TERM` / `-k` | `-k "stiftung"` | Add a term to the NGO keyword filter (repeatable) |
| `--keyword-only` | `--keyword-only -k "stiftung"` | Replace the default keyword list entirely with your `--keyword` values |

```bash
# Add on top of defaults
python scraper.py --range 1year -k "zivilgesellschaft"

# Replace defaults entirely
python scraper.py --range 1year --keyword-only -k "stiftung" -k "gemeinnützig"
```

### By party

| Flag | Example | Description |
|---|---|---|
| `--party PARTY …` / `-p` | `--party FPOE SPOE` | Keep only inquiries from these parties (space-separated) |

Accepted values (case-insensitive): `FPOE` / `FPÖ` · `SPOE` / `SPÖ` · `OEVP` / `ÖVP` · `GRUENE` / `GRÜNE` · `NEOS` · `OTHER`

```bash
python scraper.py --range 3months --party FPOE
python scraper.py --range 1year --party FPOE GRÜNE NEOS
```

### By answer status

| Flag | Description |
|---|---|
| `--answered` | Keep only inquiries that **have** been answered |
| `--unanswered` | Keep only inquiries that are still **pending** |

```bash
python scraper.py --range 6months --unanswered
```

### By content

| Flag | Example | Description |
|---|---|---|
| `--search TERM` / `-s` | `-s "Radio"` | Free-text match in title + topics (AND logic, repeatable) |
| `--exclude TERM` / `-e` | `-e "Frist"` | Drop rows containing this term (repeatable) |

```bash
# Both terms must appear
python scraper.py --range 1year -s "Radio" -s "Förderung"

# Exclude noise
python scraper.py --range 3months -e "Frist" -e "Beantwortung"
```

---

## Sorting & Shaping

| Flag | Options / Example | Description |
|---|---|---|
| `--sort-by FIELD` | `--sort-by party` | Sort by `date` (default), `party`, `number`, or `title` |
| `--sort-asc` | | Sort ascending — default is newest-first |
| `--limit N` | `--limit 20` | Cap output to N rows after all filters |
| `--offset N` | `--offset 10` | Skip the first N rows (pagination) |
| `--fields COLS` | `--fields number,date,title,party` | Only include these columns in the export |

**All available fields:** `number` · `date` · `title` · `party` · `topics` · `answered` · `detail_url` · `pdf_url` · `pdf_file` · `pdf_status`

```bash
# Sort by party, alphabetical, first 30
python scraper.py --range 1year --sort-by party --sort-asc --limit 30

# Paginate: rows 51–75
python scraper.py --range 1year --offset 50 --limit 25

# Minimal export
python scraper.py --range 3months --fields number,date,title,party --no-pdf
```

---

## Output

| Flag | Example | Description |
|---|---|---|
| `--output FORMAT` | `--output json` | `csv` (default), `json`, or `both` |
| `--output-dir DIR` | `--output-dir results/` | Where to write files (default: `output/`) |
| `--output-name NAME` | `--output-name q1_2025` | Custom base filename instead of auto-timestamped |

```bash
python scraper.py --range 1month --output both --output-dir reports/ --output-name januar_2026
```

---

## PDF Management

| Flag | Example | Description |
|---|---|---|
| `--no-pdf` | | Skip PDF download entirely — metadata only |
| `--pdf-dir DIR` | `--pdf-dir my_pdfs/` | Custom PDF directory (default: `<output-dir>/pdfs/`) |
| `--skip-existing` | | Skip download if the PDF already exists on disk |
| `--delay SECONDS` | `--delay 1.0` | Pause between PDF requests (default: `0.5`) |
| `--retry-failed CSV` | `--retry-failed output/anfragen_….csv` | Re-attempt only `FAILED` rows from a previous run; updates the CSV in-place |
| `--clean-pdfs` | | Delete all PDFs in the PDF directory and exit. If combined with `--range`/`--last`, cleans first then scrapes. |
| `--clean-output` | | Delete all `anfragen_*.csv` and `anfragen_*.json` files in the output directory and exit. If combined with `--range`/`--last`, cleans first then scrapes. |
| `--delete-pdf NUMBER` | `--delete-pdf 5771/J` | Delete the PDF for a specific inquiry number and exit |

```bash
# First run
python scraper.py --range 1month --output csv

# Re-run later, skip already-downloaded
python scraper.py --range 1month --skip-existing

# Fix only what failed last time
python scraper.py --retry-failed output/anfragen_1month_20260413_120000.csv

# Wipe all PDFs and start fresh
python scraper.py --range 1month --clean-pdfs
```

---

## Utilities & Cleanup

| Flag | Example | Description |
|---|---|---|
| `--list-output` | | List all output files with sizes and timestamps, then exit |
| `--delete-output FILENAME` | `--delete-output anfragen_1week_….csv` | Delete an output file (partial name match OK), then exit |
| `--delete-pdf NUMBER` | `--delete-pdf 5771/J` | Delete a specific inquiry's PDF, then exit |

```bash
python scraper.py --list-output
python scraper.py --delete-output anfragen_1week_20260410_120000.csv
python scraper.py --delete-pdf "5771/J"
```

---

## Common Combinations

```bash
# All FPÖ inquiries from 2025 — JSON export
python scraper.py --from 01.01.2025 --to 31.12.2025 --party FPOE --output json

# Last 100, answered only, minimal fields, no PDFs
python scraper.py --last 100 --answered --fields number,date,title,party --no-pdf

# 3-year sweep with extra keyword, skip already-downloaded PDFs, CSV + JSON
python scraper.py --range 3years -k "gemeinnützig" --skip-existing --output both

# Search for "Radio", strip noise, export both formats
python scraper.py --range 1year -s "Radio" -e "Frist" -e "Beantwortung" --output both

# Completely different topic — replace default keywords
python scraper.py --range 2years --keyword-only -k "stiftung" -k "zivilgesellschaft" --no-pdf
```

---

## Output File Fields

| Field | Description |
|---|---|
| `number` | Inquiry number, e.g. `5771/J` |
| `date` | Date filed, e.g. `10.04.2026` |
| `title` | Full inquiry title |
| `party` | Filing party: `FPÖ` · `SPÖ` · `ÖVP` · `GRÜNE` · `NEOS` · `OTHER` |
| `topics` | Topic tags from the API |
| `answered` | `True` / `False` |
| `detail_url` | Parliament detail page URL |
| `pdf_url` | Direct PDF download URL |
| `pdf_file` | Local path to the downloaded PDF |
| `pdf_status` | `OK - downloaded (…KB)` · `SKIPPED - already exists` · `FAILED - …` |

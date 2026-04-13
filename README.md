# NGO-Business Anfragen Toolkit

Two-program toolkit for researching NGO-Business parliamentary inquiries from the Austrian Parliament:

- **`scraper.py`** — fetch, filter, and download Anfragen + PDFs from the Parliament API
- **`cluster.py`** — embed all PDFs with Mistral, cluster by topic, and generate an interactive visual map

---

## Setup

### 1. Scraper dependencies
```bash
python -m pip install requests beautifulsoup4 lxml
```

### 2. Cluster dependencies (Mistral API required)

`cluster.py` calls the Mistral API directly over HTTP — no Mistral SDK needed. Install the remaining packages into your regular environment (or any venv you like):

```bash
pip install pdfplumber umap-learn scikit-learn plotly python-dotenv numpy pandas
```

### 3. Mistral API key

Add your key to `.env` (already git-ignored):
```
MISTRAL_API_KEY=your_key_here
```

Get a key at [console.mistral.ai](https://console.mistral.ai) → Workspace → API keys.

---

## Typical workflow

```bash
# Step 1 — scrape inquiries and download PDFs
python scraper.py --range 12months

# Step 2 — cluster and visualise
python cluster.py

# Open the result in your browser
start output\clusters.html
```

---

# scraper.py

Fetches NGO-related Anfragen from the Austrian Parliament API, downloads the original PDFs, and exports everything to CSV / JSON.

## How It Works

1. **Fetches** all Anfragen (type `J`) from the Parliament API across relevant legislative periods
2. **Filters** rows by NGO keywords (`ngo`, `ngo-business`, `non-governmental`, …)
3. **Deduplicates** by inquiry number — same inquiry appearing across multiple legislative periods is kept only once
4. **Scrapes** each detail page to find the "Anfrage (gescanntes Original)" PDF link — 3 fallback strategies
5. **Downloads** PDFs and reports `OK` / `SKIP` / `FAILED` per inquiry — skips files already on disk by default
6. **Exports** to `output/anfragen_<range>_<timestamp>.csv` and/or `.json`

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
| `--keyword-only` | `--keyword-only -k "stiftung"` | Replace the default keyword list entirely with your values |

```bash
python scraper.py --range 1year -k "zivilgesellschaft"
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

### By content

| Flag | Example | Description |
|---|---|---|
| `--search TERM` / `-s` | `-s "Radio"` | Free-text match in title + topics (AND logic, repeatable) |
| `--exclude TERM` / `-e` | `-e "Frist"` | Drop rows containing this term (repeatable) |

```bash
python scraper.py --range 1year -s "Radio" -s "Förderung"
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
python scraper.py --range 1year --sort-by party --sort-asc --limit 30
python scraper.py --range 1year --offset 50 --limit 25
python scraper.py --range 3months --fields number,date,title,party --no-pdf
```

---

## Output

| Flag | Example | Description |
|---|---|---|
| `--output FORMAT` | `--output json` | `csv` (default), `json`, or `both` |
| `--output-dir DIR` | `--output-dir results/` | Where to write files (default: `output/`) |
| `--output-name NAME` | `--output-name q1_2025` | Custom base filename instead of auto-timestamped |

---

## PDF Management

| Flag | Example | Description |
|---|---|---|
| `--no-pdf` | | Skip PDF download — metadata only |
| `--pdf-dir DIR` | `--pdf-dir my_pdfs/` | Custom PDF directory (default: `<output-dir>/pdfs/`) |
| `--skip-existing` | **(default)** | Skip download if the PDF already exists on disk |
| `--no-skip-existing` | | Re-download PDFs even if they already exist on disk |
| `--delay SECONDS` | `--delay 1.0` | Pause between PDF requests (default: `0.5`) |
| `--retry-failed CSV` | `--retry-failed output/anfragen_….csv` | Re-attempt only `FAILED` rows; updates the CSV in-place |
| `--clean-pdfs` | | Delete all PDFs and exit. Combined with `--range`/`--last`: cleans then scrapes. |
| `--clean-output` | | Delete all `anfragen_*.csv/.json` and exit. Combined with `--range`/`--last`: cleans then scrapes. |
| `--delete-pdf NUMBER` | `--delete-pdf 5771/J` | Delete the PDF for a specific inquiry number and exit |

```bash
# Rerun safely — PDFs already on disk are skipped automatically
python scraper.py --range 1month

# Force re-download of all PDFs
python scraper.py --range 1month --no-skip-existing
python scraper.py --retry-failed output/anfragen_1month_20260413_120000.csv
python scraper.py --clean-pdfs
python scraper.py --clean-output
```

---

## Utilities & Cleanup

| Flag | Example | Description |
|---|---|---|
| `--list-output` | | List all output files with sizes and timestamps, then exit |
| `--delete-output FILENAME` | `--delete-output anfragen_1week_….csv` | Delete an output file (partial name match OK), then exit |
| `--delete-pdf NUMBER` | `--delete-pdf 5771/J` | Delete a specific inquiry's PDF, then exit |

---

## Common Combinations

```bash
# All FPÖ inquiries from 2025 — JSON export
python scraper.py --from 01.01.2025 --to 31.12.2025 --party FPOE --output json

# Last 100, answered only, minimal fields, no PDFs
python scraper.py --last 100 --answered --fields number,date,title,party --no-pdf

# 3-year sweep with extra keyword (PDFs already on disk are skipped automatically)
python scraper.py --range 3years -k "gemeinnützig" --output both

# Search for "Radio", strip noise
python scraper.py --range 1year -s "Radio" -e "Frist" -e "Beantwortung" --output both

# Nuclear reset — wipe everything, then fresh 12-month run
python scraper.py --clean-output --clean-pdfs --range 12months
```

---

## Scraper Output Fields

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

---

---

# cluster.py

Reads the downloaded PDFs (or falls back to titles), embeds them with the Mistral API, clusters them by topic, names each cluster with Mistral Large, and produces a self-contained interactive HTML map.

## How It Works

1. **Extracts text** from each PDF via `pdfplumber` — automatically falls back to `title + topics` from the most recent CSV when a PDF is a scanned image (common for "gescanntes Original")
2. **Embeds** all documents in batches using `mistral-embed` (1024-dimensional vectors)
3. **Reduces** dimensions with UMAP — one high-dimensional pass for clustering quality, one 2D pass for the plot
4. **Clusters** with HDBSCAN — no need to pick K; cluster count is discovered automatically
5. **Names** each cluster with Mistral Large based on a sample of its titles (output in German)
6. **Exports** an interactive Plotly HTML file and a `clusters.csv` assignment table

## Mistral API

`cluster.py` calls the Mistral REST API directly over HTTP using `requests` — no SDK installed. Both endpoints use the same API key from `.env`:

| Endpoint | Model | Purpose |
|---|---|---|
| `POST /v1/embeddings` | `mistral-embed` | Convert document text to 1024-dim vectors |
| `POST /v1/chat/completions` | `mistral-large-latest` | Generate a short German name for each cluster |

## Usage

```bash
# Default — reads output/pdfs/, writes output/clusters.html + output/clusters.csv
python cluster.py

# Scanned PDFs / faster — use titles + topics instead of PDF text
python cluster.py --no-pdf-text

# Adjust cluster granularity (smaller = more clusters)
python cluster.py --min-cluster-size 4

# Custom paths and output name
python cluster.py --pdf-dir my_pdfs/ --output-name ngo_map_april2026

# Delete clusters.csv if it crashes VS Code when hovered
python cluster.py --clean
```

## cluster.py Flags

| Flag | Default | Description |
|---|---|---|
| `--pdf-dir DIR` | `output/pdfs/` | Directory of PDFs to embed and cluster |
| `--output-dir DIR` | `output/` | Where to write `clusters.html` and `clusters.csv` |
| `--output-name NAME` | `clusters` | Base name for output files (no extension) |
| `--min-cluster-size N` | `3` | Minimum documents per HDBSCAN cluster — increase for fewer, broader clusters |
| `--no-pdf-text` | off | Skip PDF text extraction; use `title + topics` from CSV only |
| `--clean` | | Delete the output CSV (`clusters.csv`) and exit — useful if the file crashes your editor |

## The Interactive Map

The output `clusters.html` is a self-contained file — open it in any browser, no server needed.

- **Zoom & pan** with scroll wheel and drag
- **Hover** over any point to see: inquiry number, title, party, date, cluster name
- **Click** any point to open the parliament detail page in a new tab
- **Toggle clusters** on/off by clicking items in the legend
- Points coloured by cluster; cluster names labelled at centroids

## Cluster Output Fields (`clusters.csv`)

| Field | Description |
|---|---|
| `cluster_id` | Numeric cluster ID (`-1` = unassigned noise) |
| `cluster_name` | German name generated by Mistral Large |
| `number` | Inquiry number |
| `date` | Date filed |
| `title` | Full inquiry title |
| `party` | Filing party |
| `topics` | Topic tags |
| `text_source` | `pdf` if text was extracted from the PDF, `title` if fallback was used |
| `detail_url` | Parliament detail page URL |

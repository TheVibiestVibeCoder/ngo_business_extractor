#!/usr/bin/env python3
"""
NGO-Business Anfragen Scraper
Scrapes Austrian Parliament inquiries (Anfragen) related to NGO topics,
downloads associated PDFs, and exports all data to CSV/JSON.

Usage examples:
  python scraper.py --range 12months
  python scraper.py --range 3years --output json
  python scraper.py --last 10
  python scraper.py --last 50 --no-pdf

  # Filtering
  python scraper.py --range 3months --party FPOE SPOE
  python scraper.py --range 1year --answered
  python scraper.py --range 6months --search "Radio" --exclude "Frist"
  python scraper.py --range 1year --keyword "zivilgesellschaft" --keyword "verein"
  python scraper.py --range 12months --keyword-only --keyword "stiftung"
  python scraper.py --from 01.01.2025 --to 31.03.2025

  # Sorting & shaping
  python scraper.py --range 1year --sort-by party --sort-asc
  python scraper.py --last 100 --limit 20 --offset 10
  python scraper.py --range 6months --fields number,date,title,party

  # PDF management
  python scraper.py --range 1month --skip-existing
  python scraper.py --retry-failed output/anfragen_1month_20260413.csv
  python scraper.py --range 3months --pdf-dir my_pdfs/

  # Cleanup utilities
  python scraper.py --range 1month --clean-pdfs
  python scraper.py --list-output
  python scraper.py --delete-output anfragen_1week_20260410_120000.csv
  python scraper.py --delete-pdf 5771/J
"""

import argparse
import csv
import glob
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://www.parlament.gv.at"
API_URL = f"{BASE_URL}/Filter/api/filter/data/101?js=eval&showAll=true"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

DEFAULT_NGO_KEYWORDS = [
    "ngo",
    "ngos",
    "ngo-business",
    "ngo business",
    "nicht-regierungsorganisation",
    "nicht regierungsorganisation",
    "nichtregierungsorganisation",
    "non-governmental",
    "nonprofit",
    "non-profit",
    "ehrenamtlich",
]

# Ordered from oldest to newest
GP_CODES_BY_AGE = ["XXIV", "XXV", "XXVI", "XXVII", "XXVIII"]

RANGE_CONFIG = {
    "1week":    {"delta": timedelta(weeks=1),    "gp_count": 1},
    "1month":   {"delta": timedelta(days=30),    "gp_count": 1},
    "3months":  {"delta": timedelta(days=91),    "gp_count": 1},
    "6months":  {"delta": timedelta(days=182),   "gp_count": 2},
    "12months": {"delta": timedelta(days=365),   "gp_count": 2},
    "1year":    {"delta": timedelta(days=365),   "gp_count": 2},
    "2years":   {"delta": timedelta(days=730),   "gp_count": 2},
    "3years":   {"delta": timedelta(days=1095),  "gp_count": 3},
    "5years":   {"delta": timedelta(days=1825),  "gp_count": 4},
    "10years":  {"delta": timedelta(days=3650),  "gp_count": 5},
}

ALL_FIELDS = ["number", "date", "title", "party", "topics",
              "answered", "detail_url", "pdf_url", "pdf_file", "pdf_status"]

PARTY_ALIASES = {
    "fpoe": "FPÖ", "fpo": "FPÖ", "fpö": "FPÖ", "fpö": "FPÖ", "f": "FPÖ",
    "spoe": "SPÖ", "spo": "SPÖ", "spö": "SPÖ", "s": "SPÖ",
    "oevp": "ÖVP", "ovp": "ÖVP", "övp": "ÖVP", "v": "ÖVP",
    "gruene": "GRÜNE", "grüne": "GRÜNE", "gruenen": "GRÜNE", "g": "GRÜNE",
    "neos": "NEOS", "n": "NEOS",
    "other": "OTHER",
}

# Row field indices
IDX_DATE   = 4
IDX_TITLE  = 6
IDX_NUMBER = 7
IDX_LINK   = 14
IDX_PARTY  = 21
IDX_TOPICS = 22


# ── Logging ───────────────────────────────────────────────────────────────────

def _safe_print(*args, file=None, **kwargs):
    text = " ".join(str(a) for a in args)
    try:
        print(text, file=file, **kwargs)
    except UnicodeEncodeError:
        print(text.encode("ascii", errors="replace").decode("ascii"), file=file, **kwargs)


def log(msg: str):
    _safe_print(f"  * {msg}", flush=True)


def log_ok(msg: str):
    _safe_print(f"  [OK]   {msg}", flush=True)


def log_err(msg: str):
    _safe_print(f"  [FAIL] {msg}", flush=True, file=sys.stderr)


def log_skip(msg: str):
    _safe_print(f"  [SKIP] {msg}", flush=True)


def section(title: str = ""):
    if title:
        _safe_print(f"\n  -- {title} " + "-" * max(0, 52 - len(title)))
    else:
        _safe_print("  " + "-" * 58)


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_party(party_json: str) -> str:
    try:
        parties = json.loads(party_json or "[]")
    except (json.JSONDecodeError, TypeError):
        return "OTHER"
    if not isinstance(parties, list):
        return "OTHER"
    p = " ".join(parties).upper()
    if "SPÖ" in p or "SOZIALDEMOKRAT" in p:
        return "SPÖ"
    if "ÖVP" in p or "VOLKSPARTEI" in p:
        return "ÖVP"
    if "FPÖ" in p or "FREIHEITLICH" in p:
        return "FPÖ"
    if "GRÜNE" in p or "GRÜNEN" in p:
        return "GRÜNE"
    if "NEOS" in p:
        return "NEOS"
    return "OTHER"


def is_answered(title: str) -> bool:
    """Returns True when the inquiry title indicates it has been answered."""
    return bool(re.search(r"beantwortet", title, re.IGNORECASE))


def safe_filename(title: str, number: str) -> str:
    safe = re.sub(r"[^\w\s\-]", "", title, flags=re.UNICODE)
    safe = re.sub(r"\s+", "_", safe.strip())[:60]
    return f"{number}_{safe}.pdf".replace("/", "-")


def normalize_party(raw: str) -> str:
    """Normalize user-supplied party names to canonical form."""
    return PARTY_ALIASES.get(raw.lower().strip(), raw.upper().strip())


# ── API fetch ─────────────────────────────────────────────────────────────────

def fetch_rows(gp_codes: list[str], timeout: int = 30) -> list:
    payload = {"GP_CODE": gp_codes, "VHG": ["J_JPR_M"], "DOKTYP": ["J"]}
    try:
        resp = requests.post(API_URL, json=payload, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.json().get("rows", [])
    except requests.RequestException as exc:
        log_err(f"API request failed: {exc}")
        return []
    except (json.JSONDecodeError, KeyError) as exc:
        log_err(f"Failed to parse API response: {exc}")
        return []


# ── Detail page PDF scraping ──────────────────────────────────────────────────

def fetch_pdf_link(detail_url: str, timeout: int = 15) -> tuple[str | None, str]:
    try:
        resp = requests.get(detail_url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return None, f"HTTP error: {exc}"

    soup = BeautifulSoup(resp.text, "lxml")

    # Strategy 1: <li> labelled "Anfrage (gescanntes Original)"
    for li in soup.find_all("li"):
        label_texts = [s.get_text(strip=True) for s in li.find_all("span")]
        if any("Anfrage" in t and ("Original" in t or "gescannt" in t) for t in label_texts):
            link_tag = li.find("a", href=re.compile(r"\.pdf", re.I))
            if link_tag and link_tag.get("href"):
                return urljoin(BASE_URL, link_tag["href"]), "ok"

    all_pdf = soup.find_all("a", href=re.compile(r"\.pdf", re.I))

    # Strategy 2: Parliament Anfrage URL pattern
    for tag in all_pdf:
        href = tag.get("href", "")
        if re.search(r"/dokument/[A-Z]+/J/", href):
            return urljoin(BASE_URL, href), "ok (fallback)"

    # Strategy 3: first PDF found
    if all_pdf:
        return urljoin(BASE_URL, all_pdf[0].get("href", "")), "ok (first-pdf fallback)"

    return None, "no PDF link found on detail page"


def download_pdf(pdf_url: str, dest_path: Path, timeout: int = 30) -> tuple[bool, str]:
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=timeout, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "pdf" not in ct.lower() and not pdf_url.lower().endswith(".pdf"):
            return False, f"unexpected content-type: {ct}"
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True, f"downloaded ({dest_path.stat().st_size / 1024:.1f} KB)"
    except requests.RequestException as exc:
        return False, f"download error: {exc}"
    except OSError as exc:
        return False, f"file write error: {exc}"


# ── Filtering pipeline ────────────────────────────────────────────────────────

def build_keyword_list(extra_keywords: list[str], keyword_only: bool) -> list[str]:
    base = [] if keyword_only else list(DEFAULT_NGO_KEYWORDS)
    return base + [k.lower() for k in (extra_keywords or [])]


def apply_filters(
    results: list[dict],
    *,
    parties: list[str] | None,
    answered: bool | None,       # True = only answered, False = only unanswered, None = all
    search_terms: list[str] | None,
    exclude_terms: list[str] | None,
    from_date: datetime | None,
    to_date: datetime | None,
) -> list[dict]:
    out = []
    for rec in results:
        # Party filter
        if parties and rec["party"] not in parties:
            continue

        # Answered/unanswered filter
        if answered is True and not rec["answered"]:
            continue
        if answered is False and rec["answered"]:
            continue

        # Explicit date bounds
        row_date = parse_date(rec["date"])
        if from_date and row_date and row_date < from_date:
            continue
        if to_date and row_date and row_date > to_date:
            continue

        # Free-text search (ALL terms must match)
        if search_terms:
            haystack = (rec["title"] + " " + rec["topics"]).lower()
            if not all(term.lower() in haystack for term in search_terms):
                continue

        # Exclude filter (ANY term causes exclusion)
        if exclude_terms:
            haystack = (rec["title"] + " " + rec["topics"]).lower()
            if any(term.lower() in haystack for term in exclude_terms):
                continue

        out.append(rec)
    return out


# ── Core row processing ───────────────────────────────────────────────────────

def rows_to_records(
    rows: list,
    keywords: list[str],
    cutoff_date: datetime | None,
) -> list[dict]:
    """Convert raw API rows → filtered record dicts (no PDFs yet)."""
    results = []
    for row in rows:
        if len(row) <= max(IDX_DATE, IDX_TITLE, IDX_NUMBER, IDX_LINK, IDX_PARTY, IDX_TOPICS):
            continue

        date_str   = row[IDX_DATE]   or ""
        title      = row[IDX_TITLE]  or ""
        number     = str(row[IDX_NUMBER] or "")
        link       = row[IDX_LINK]   or ""
        party_json = row[IDX_PARTY]  or "[]"
        topics_raw = row[IDX_TOPICS] or ""

        searchable = f"{title} {topics_raw}".lower()
        if not any(kw in searchable for kw in keywords):
            continue

        row_date = parse_date(date_str)
        if cutoff_date and row_date and row_date < cutoff_date:
            continue

        results.append({
            "number":     number,
            "date":       date_str,
            "title":      title,
            "party":      parse_party(party_json),
            "topics":     topics_raw,
            "answered":   is_answered(title),
            "detail_url": urljoin(BASE_URL, link) if link else "",
            "pdf_url":    "",
            "pdf_file":   "",
            "pdf_status": "not_attempted",
        })

    # Deduplicate by inquiry number (same inquiry can appear in multiple GP periods)
    seen: set[str] = set()
    unique: list[dict] = []
    for rec in results:
        if rec["number"] in seen:
            continue
        seen.add(rec["number"])
        unique.append(rec)
    if len(unique) < len(results):
        log_skip(f"Dropped {len(results) - len(unique)} duplicate inquiry number(s)")
    results = unique

    results.sort(key=lambda r: parse_date(r["date"]) or datetime.min, reverse=True)
    return results


# ── PDF pass ──────────────────────────────────────────────────────────────────

def run_pdf_pass(
    results: list[dict],
    pdf_dir: Path,
    delay: float,
    skip_existing: bool,
) -> None:
    """Mutates records in-place: adds pdf_url, pdf_file, pdf_status."""
    total = len(results)
    section(f"Downloading PDFs ({total})")
    print()
    for i, rec in enumerate(results, 1):
        num_label = rec["number"] or f"row{i}"
        _safe_print(f"  [{i:3d}/{total}] {num_label}: {rec['title'][:52]}")

        if not rec["detail_url"]:
            rec["pdf_status"] = "FAILED - no detail URL"
            log_err("no detail URL")
            continue

        filename = safe_filename(rec["title"], num_label)
        dest = pdf_dir / filename
        rec["pdf_file"] = str(dest)

        if skip_existing and dest.exists():
            rec["pdf_url"]    = rec.get("pdf_url") or ""
            rec["pdf_status"] = f"SKIPPED - already exists ({dest.stat().st_size // 1024} KB)"
            log_skip(f"already exists: {dest.name}")
            continue

        pdf_url, find_msg = fetch_pdf_link(rec["detail_url"])
        if not pdf_url:
            rec["pdf_status"] = f"FAILED - {find_msg}"
            log_err(find_msg)
            time.sleep(delay)
            continue

        rec["pdf_url"] = pdf_url
        ok, dl_msg = download_pdf(pdf_url, dest)
        if ok:
            rec["pdf_status"] = f"OK - {dl_msg}"
            log_ok(dl_msg)
        else:
            rec["pdf_status"] = f"FAILED - {dl_msg}"
            log_err(dl_msg)

        time.sleep(delay)


# ── Output ────────────────────────────────────────────────────────────────────

def project_fields(results: list[dict], fields: list[str] | None) -> list[dict]:
    if not fields:
        return results
    return [{f: r.get(f, "") for f in fields} for r in results]


def write_csv(results: list[dict], path: Path):
    if not results:
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)


def write_json(results: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


# ── Utility commands ──────────────────────────────────────────────────────────

def cmd_list_output(output_dir: Path):
    section("Output files")
    files = sorted(output_dir.glob("anfragen_*.csv")) + sorted(output_dir.glob("anfragen_*.json"))
    if not files:
        log("No output files found.")
        return
    for f in files:
        size = f.stat().st_size / 1024
        mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%d.%m.%Y %H:%M")
        _safe_print(f"    {f.name:<55}  {size:7.1f} KB  {mtime}")


def cmd_delete_output(output_dir: Path, filename: str):
    path = output_dir / filename
    if not path.exists():
        # try glob
        matches = list(output_dir.glob(f"*{filename}*"))
        if not matches:
            log_err(f"File not found: {filename}")
            return
        if len(matches) > 1:
            log_err(f"Ambiguous match — {len(matches)} files found for '{filename}'. Be more specific:")
            for m in matches:
                _safe_print(f"    {m.name}")
            return
        path = matches[0]
    path.unlink()
    log_ok(f"Deleted: {path.name}")


def cmd_delete_pdf(pdf_dir: Path, number: str):
    """Delete the PDF(s) matching an inquiry number like '5771/J'."""
    safe_num = number.replace("/", "-")
    matches = list(pdf_dir.glob(f"{safe_num}_*.pdf"))
    if not matches:
        matches = list(pdf_dir.glob(f"*{safe_num}*"))
    if not matches:
        log_err(f"No PDF found for inquiry number: {number}")
        return
    for p in matches:
        p.unlink()
        log_ok(f"Deleted PDF: {p.name}")


def cmd_clean_pdfs(pdf_dir: Path):
    if not pdf_dir.exists():
        log("PDF directory does not exist, nothing to clean.")
        return
    count = sum(1 for _ in pdf_dir.glob("*.pdf"))
    if count == 0:
        log("No PDFs found.")
        return
    answer = input(f"  Delete all {count} PDF(s) in {pdf_dir}? [y/N] ").strip().lower()
    if answer == "y":
        shutil.rmtree(pdf_dir)
        pdf_dir.mkdir(parents=True)
        log_ok(f"Deleted {count} PDF(s) and recreated empty directory.")
    else:
        log("Aborted.")


def cmd_clean_output(output_dir: Path):
    files = list(output_dir.glob("anfragen_*.csv")) + list(output_dir.glob("anfragen_*.json"))
    if not files:
        log("No output files found.")
        return
    _safe_print(f"\n  Files that will be deleted:")
    for f in sorted(files):
        _safe_print(f"    {f.name}")
    answer = input(f"\n  Delete all {len(files)} output file(s) in {output_dir}? [y/N] ").strip().lower()
    if answer == "y":
        for f in files:
            f.unlink()
        log_ok(f"Deleted {len(files)} output file(s).")
    else:
        log("Aborted.")


def cmd_retry_failed(csv_path: Path, pdf_dir: Path, delay: float, skip_existing: bool):
    """Load a previous CSV and retry only rows with a FAILED pdf_status."""
    if not csv_path.exists():
        log_err(f"CSV file not found: {csv_path}")
        sys.exit(1)

    with open(csv_path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    failed = [r for r in rows if r.get("pdf_status", "").startswith("FAILED")]
    if not failed:
        log_ok(f"No failed PDFs found in {csv_path.name}")
        return

    log(f"Retrying {len(failed)} failed PDF(s) from {csv_path.name} …")

    # Convert answered field back to bool
    for r in failed:
        if isinstance(r.get("answered"), str):
            r["answered"] = r["answered"].lower() in ("true", "1", "yes")
        r["pdf_url"] = ""
        r["pdf_file"] = ""
        r["pdf_status"] = "not_attempted"

    run_pdf_pass(failed, pdf_dir, delay, skip_existing)

    # Write updated CSV back (merge retried rows by number)
    num_to_new = {r["number"]: r for r in failed}
    for row in rows:
        if row["number"] in num_to_new:
            row.update(num_to_new[row["number"]])

    write_csv(rows, csv_path)
    log_ok(f"Updated CSV: {csv_path}")

    ok_count   = sum(1 for r in failed if r["pdf_status"].startswith("OK"))
    fail_count = sum(1 for r in failed if r["pdf_status"].startswith("FAILED"))
    log_ok(f"Retry result: {ok_count} succeeded, {fail_count} still failed")


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scraper.py",
        description="Scrape NGO-related Anfragen from the Austrian Parliament API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Time / scope (mutually exclusive) ─────────────────────────────────────
    time_group = parser.add_mutually_exclusive_group()
    time_group.add_argument(
        "--range",
        choices=list(RANGE_CONFIG.keys()),
        metavar="RANGE",
        help="Time range. Choices: " + ", ".join(RANGE_CONFIG.keys()),
    )
    time_group.add_argument(
        "--last",
        type=int,
        metavar="N",
        help="Fetch the last N inquiries (newest first).",
    )
    time_group.add_argument(
        "--from",
        dest="from_date",
        metavar="DD.MM.YYYY",
        help="Explicit start date (use with --to or alone).",
    )

    parser.add_argument(
        "--to",
        dest="to_date",
        metavar="DD.MM.YYYY",
        help="Explicit end date (use together with --from or --range).",
    )

    # ── Keyword filtering ──────────────────────────────────────────────────────
    kw_group = parser.add_argument_group("keyword filtering")
    kw_group.add_argument(
        "--keyword", "-k",
        action="append",
        metavar="TERM",
        help="Add a keyword to the NGO filter. Repeatable: -k ngo -k stiftung",
    )
    kw_group.add_argument(
        "--keyword-only",
        action="store_true",
        help="Replace default NGO keywords entirely with --keyword values.",
    )

    # ── Content filtering ──────────────────────────────────────────────────────
    cf_group = parser.add_argument_group("content filtering")
    cf_group.add_argument(
        "--party", "-p",
        nargs="+",
        metavar="PARTY",
        help=(
            "Keep only inquiries from these parties. "
            "Accepts: FPOE/FPÖ, SPOE/SPÖ, OEVP/ÖVP, GRUENE/GRÜNE, NEOS, OTHER. "
            "Repeatable or space-separated: --party FPOE SPOE"
        ),
    )
    cf_group.add_argument(
        "--answered",
        action="store_true",
        default=None,
        help="Keep only inquiries that have been answered.",
    )
    cf_group.add_argument(
        "--unanswered",
        action="store_true",
        default=None,
        help="Keep only inquiries that have NOT been answered.",
    )
    cf_group.add_argument(
        "--search", "-s",
        action="append",
        metavar="TERM",
        help="Free-text search in title+topics (AND logic). Repeatable.",
    )
    cf_group.add_argument(
        "--exclude", "-e",
        action="append",
        metavar="TERM",
        help="Exclude rows where title+topics contain this term. Repeatable.",
    )

    # ── Sorting & shaping ──────────────────────────────────────────────────────
    sh_group = parser.add_argument_group("sorting & shaping")
    sh_group.add_argument(
        "--sort-by",
        choices=["date", "party", "number", "title"],
        default="date",
        help="Sort results by this field (default: date).",
    )
    sh_group.add_argument(
        "--sort-asc",
        action="store_true",
        help="Sort ascending (default is descending / newest first).",
    )
    sh_group.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Cap output to N rows after all filters.",
    )
    sh_group.add_argument(
        "--offset",
        type=int,
        default=0,
        metavar="N",
        help="Skip the first N rows before applying --limit (default: 0).",
    )
    sh_group.add_argument(
        "--fields",
        metavar="FIELDS",
        help=(
            "Comma-separated list of columns to include in the export. "
            "Available: " + ", ".join(ALL_FIELDS)
        ),
    )

    # ── Output ─────────────────────────────────────────────────────────────────
    out_group = parser.add_argument_group("output")
    out_group.add_argument(
        "--output",
        choices=["csv", "json", "both"],
        default="csv",
        help="Output format (default: csv).",
    )
    out_group.add_argument(
        "--output-dir",
        default="output",
        metavar="DIR",
        help="Directory for output files (default: output/).",
    )
    out_group.add_argument(
        "--output-name",
        metavar="NAME",
        help="Custom base name for output file(s), without extension.",
    )

    # ── PDF flags ──────────────────────────────────────────────────────────────
    pdf_group = parser.add_argument_group("PDF management")
    pdf_group.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip PDF download entirely.",
    )
    pdf_group.add_argument(
        "--pdf-dir",
        metavar="DIR",
        help="Custom PDF output directory (default: <output-dir>/pdfs/).",
    )
    pdf_group.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip PDF if a file with that name already exists on disk (default: on). Use --no-skip-existing to re-download.",
    )
    pdf_group.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Re-download PDFs even if they already exist on disk.",
    )
    pdf_group.add_argument(
        "--retry-failed",
        metavar="CSV",
        help="Load a previous CSV and retry only rows with FAILED pdf_status.",
    )
    pdf_group.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds between PDF requests (default: 0.5).",
    )

    # ── Utility / cleanup commands ─────────────────────────────────────────────
    util_group = parser.add_argument_group("utilities & cleanup")
    util_group.add_argument(
        "--clean-pdfs",
        action="store_true",
        help="Delete all PDFs in the PDF directory. Standalone: exits after. Combined with --range/--last: cleans then scrapes.",
    )
    util_group.add_argument(
        "--clean-output",
        action="store_true",
        help="Delete all anfragen_*.csv and anfragen_*.json files in the output directory. Standalone: exits after. Combined with --range/--last: cleans then scrapes.",
    )
    util_group.add_argument(
        "--list-output",
        action="store_true",
        help="List all output files and exit.",
    )
    util_group.add_argument(
        "--delete-output",
        metavar="FILENAME",
        help="Delete a specific output file by name (partial match OK) and exit.",
    )
    util_group.add_argument(
        "--delete-pdf",
        metavar="NUMBER",
        help="Delete the PDF for a specific inquiry number (e.g. 5771/J) and exit.",
    )

    return parser


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = build_parser()
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = Path(args.pdf_dir) if args.pdf_dir else (output_dir / "pdfs")

    # ── Pure utility commands (exit early) ────────────────────────────────────
    if args.list_output:
        cmd_list_output(output_dir)
        return

    if args.delete_output:
        cmd_delete_output(output_dir, args.delete_output)
        return

    if args.delete_pdf:
        cmd_delete_pdf(pdf_dir, args.delete_pdf)
        return

    if args.clean_pdfs and not args.range and not args.last and not args.from_date:
        cmd_clean_pdfs(pdf_dir)
        return

    if args.clean_output and not args.range and not args.last and not args.from_date:
        cmd_clean_output(output_dir)
        return

    if args.retry_failed:
        if args.clean_pdfs:
            cmd_clean_pdfs(pdf_dir)
        cmd_retry_failed(
            Path(args.retry_failed), pdf_dir,
            delay=args.delay, skip_existing=args.skip_existing,
        )
        return

    # ── Validate: need at least one time selector ─────────────────────────────
    if not args.range and not args.last and not args.from_date:
        parser.error("Provide one of: --range, --last, or --from [--to].")

    # ── Resolve dates & GP codes ───────────────────────────────────────────────
    now = datetime.now()
    cutoff_date: datetime | None = None
    to_date: datetime | None = None
    gp_codes: list[str]
    range_label: str

    if args.range:
        cfg = RANGE_CONFIG[args.range]
        cutoff_date = now - cfg["delta"]
        gp_codes = GP_CODES_BY_AGE[-cfg["gp_count"]:]
        range_label = args.range
    elif args.from_date:
        cutoff_date = parse_date(args.from_date)
        if not cutoff_date:
            parser.error(f"Cannot parse --from date: {args.from_date}")
        # Determine GP codes based on how far back we're going
        years_back = (now - cutoff_date).days / 365
        gp_count = max(1, min(len(GP_CODES_BY_AGE), int(years_back) + 1))
        gp_codes = GP_CODES_BY_AGE[-gp_count:]
        range_label = f"from-{cutoff_date.strftime('%Y%m%d')}"
    else:
        gp_codes = GP_CODES_BY_AGE[-2:]
        range_label = f"last-{args.last}"

    if args.to_date:
        to_date = parse_date(args.to_date)
        if not to_date:
            parser.error(f"Cannot parse --to date: {args.to_date}")
        # set end of day
        to_date = to_date.replace(hour=23, minute=59, second=59)

    # ── Build keyword list ─────────────────────────────────────────────────────
    keywords = build_keyword_list(args.keyword, args.keyword_only)
    if not keywords:
        parser.error("No keywords defined. Use --keyword to add at least one.")

    # ── Normalise party filter ─────────────────────────────────────────────────
    party_filter: list[str] | None = None
    if args.party:
        party_filter = [normalize_party(p) for p in args.party]

    # ── answered filter ────────────────────────────────────────────────────────
    answered_filter: bool | None = None
    if args.answered:
        answered_filter = True
    elif args.unanswered:
        answered_filter = False

    # ── Fields list ───────────────────────────────────────────────────────────
    fields_filter: list[str] | None = None
    if args.fields:
        fields_filter = [f.strip() for f in args.fields.split(",")]
        bad = [f for f in fields_filter if f not in ALL_FIELDS]
        if bad:
            parser.error(f"Unknown field(s): {', '.join(bad)}. Available: {', '.join(ALL_FIELDS)}")

    # ── PDF setup ─────────────────────────────────────────────────────────────
    do_pdf = not args.no_pdf
    if args.clean_output:
        cmd_clean_output(output_dir)
    if args.clean_pdfs and do_pdf:
        cmd_clean_pdfs(pdf_dir)

    # ── Banner ────────────────────────────────────────────────────────────────
    print()
    section()
    _safe_print("  NGO-Business Anfragen Scraper")
    section()
    if args.range:
        _safe_print(f"  Range         : {args.range}")
        _safe_print(f"  Cutoff date   : {cutoff_date.strftime('%d.%m.%Y')}")
    elif args.from_date:
        _safe_print(f"  From date     : {cutoff_date.strftime('%d.%m.%Y')}")
        if to_date:
            _safe_print(f"  To date       : {to_date.strftime('%d.%m.%Y')}")
    else:
        _safe_print(f"  Mode          : last {args.last} inquiries")
    _safe_print(f"  GP codes      : {', '.join(gp_codes)}")
    _safe_print(f"  Keywords      : {', '.join(keywords[:5])}{'…' if len(keywords) > 5 else ''}")
    if party_filter:
        _safe_print(f"  Party filter  : {', '.join(party_filter)}")
    if answered_filter is True:
        _safe_print(f"  Status filter : answered only")
    if answered_filter is False:
        _safe_print(f"  Status filter : unanswered only")
    if args.search:
        _safe_print(f"  Search        : {', '.join(args.search)}")
    if args.exclude:
        _safe_print(f"  Exclude       : {', '.join(args.exclude)}")
    if args.sort_by != "date" or args.sort_asc:
        direction = "asc" if args.sort_asc else "desc"
        _safe_print(f"  Sort          : {args.sort_by} {direction}")
    if args.limit:
        _safe_print(f"  Limit         : {args.limit} (offset {args.offset})")
    _safe_print(f"  PDFs          : {'yes -> ' + str(pdf_dir) if do_pdf else 'skipped'}")
    _safe_print(f"  Output dir    : {output_dir}")
    section()
    print()

    # ── Fetch ─────────────────────────────────────────────────────────────────
    log("Fetching from Parliament API ...")
    rows = fetch_rows(gp_codes)
    if not rows:
        log_err("No rows returned from API. Exiting.")
        sys.exit(1)
    log_ok(f"{len(rows):,} total rows fetched from API")

    # ── Build records ─────────────────────────────────────────────────────────
    log("Filtering by NGO keywords and date range ...")
    results = rows_to_records(rows, keywords, cutoff_date)
    log_ok(f"{len(results)} NGO-keyword matches after date filter")

    # ── Apply secondary filters ────────────────────────────────────────────────
    results = apply_filters(
        results,
        parties=party_filter,
        answered=answered_filter,
        search_terms=args.search,
        exclude_terms=args.exclude,
        from_date=cutoff_date if args.from_date else None,
        to_date=to_date,
    )
    log_ok(f"{len(results)} records after content filters")

    if not results:
        log_err("No matching inquiries found.")
        sys.exit(0)

    # ── Sort ──────────────────────────────────────────────────────────────────
    sort_key_fn = {
        "date":   lambda r: parse_date(r["date"]) or datetime.min,
        "party":  lambda r: r["party"],
        "number": lambda r: r["number"],
        "title":  lambda r: r["title"].lower(),
    }[args.sort_by]
    results.sort(key=sort_key_fn, reverse=not args.sort_asc)

    # ── --last N (applied after sort so we get the most recent N) ─────────────
    if args.last is not None:
        results = results[:args.last]

    # ── --offset / --limit ────────────────────────────────────────────────────
    if args.offset:
        results = results[args.offset:]
    if args.limit:
        results = results[:args.limit]

    log_ok(f"{len(results)} final records")

    # ── PDF pass ──────────────────────────────────────────────────────────────
    if do_pdf:
        run_pdf_pass(results, pdf_dir, args.delay, args.skip_existing)

    # ── Write output ──────────────────────────────────────────────────────────
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    base_name = args.output_name or f"anfragen_{range_label}_{timestamp}"

    export_data = project_fields(results, fields_filter)

    if args.output in ("csv", "both"):
        csv_path = output_dir / f"{base_name}.csv"
        write_csv(export_data, csv_path)
        log_ok(f"CSV saved -> {csv_path}")

    if args.output in ("json", "both"):
        json_path = output_dir / f"{base_name}.json"
        write_json(export_data, json_path)
        log_ok(f"JSON saved -> {json_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    if do_pdf:
        ok_cnt   = sum(1 for r in results if r["pdf_status"].startswith("OK"))
        skip_cnt = sum(1 for r in results if r["pdf_status"].startswith("SKIP"))
        fail_cnt = sum(1 for r in results if r["pdf_status"].startswith("FAILED"))
        print()
        section("PDF Summary")
        _safe_print(f"  [OK]   Downloaded : {ok_cnt}")
        if skip_cnt:
            _safe_print(f"  [SKIP] Skipped    : {skip_cnt}")
        _safe_print(f"  [FAIL] Failed     : {fail_cnt}")
        section()

    print()
    _safe_print(f"  Done. {len(results)} inquiries exported.")
    print()


if __name__ == "__main__":
    main()

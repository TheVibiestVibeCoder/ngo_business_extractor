#!/usr/bin/env python3
"""
cluster.py — Topic-cluster NGO Anfragen PDFs using Mistral embeddings.

Pipeline:
  1. Extract text from PDFs in output/pdfs/  (falls back to titles from CSV)
  2. Embed all documents with mistral-embed
  3. Reduce dimensions with UMAP
  4. Cluster with HDBSCAN
  5. Name each cluster with Mistral Large
  6. Export an interactive HTML scatter map (zoom, hover, click-to-open)
  7. Export a cluster assignment CSV alongside the HTML

Usage:
  # Activate the venv first (required — packages are installed there):
  #   Windows:  C:\\ngo_venv\\Scripts\\activate
  #   Then:     python cluster.py

  python cluster.py
  python cluster.py --pdf-dir output/pdfs --min-cluster-size 4
  python cluster.py --no-pdf-text          # use titles only (faster, good for scanned PDFs)
  python cluster.py --output-name my_map   # saves my_map.html + my_map.csv
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pdfplumber
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Model config ───────────────────────────────────────────────────────────────

EMBED_MODEL    = "mistral-embed"
CHAT_MODEL     = "mistral-large-latest"
EMBED_BATCH    = 32     # items per embedding request
MAX_TEXT_CHARS = 4000   # chars per doc fed to the embedder

# ── Colour palette for clusters ───────────────────────────────────────────────

PALETTE = [
    "#e63946", "#2a9d8f", "#f4a261", "#457b9d", "#e9c46a",
    "#6a0572", "#52b788", "#f77f00", "#a8dadc", "#d62828",
    "#023e8a", "#e76f51", "#fcbf49", "#264653", "#6d6875",
    "#b5838d", "#00b4d8", "#80b918", "#ff6b6b", "#c77dff",
]

# ── Logging ───────────────────────────────────────────────────────────────────

def _p(*args, file=None, **kw):
    text = " ".join(str(a) for a in args)
    try:
        print(text, file=file, **kw)
    except UnicodeEncodeError:
        print(text.encode("ascii", errors="replace").decode(), file=file, **kw)

def log(msg):      _p(f"  * {msg}", flush=True)
def log_ok(msg):   _p(f"  [OK]   {msg}", flush=True)
def log_err(msg):  _p(f"  [FAIL] {msg}", flush=True, file=sys.stderr)
def log_skip(msg): _p(f"  [SKIP] {msg}", flush=True)
def section(t=""):
    if t:
        _p(f"\n  -- {t} " + "-" * max(0, 52 - len(t)))
    else:
        _p("  " + "-" * 58)


# ── PDF text extraction ───────────────────────────────────────────────────────

def extract_pdf_text(path: Path) -> str:
    """Try to extract selectable text from a PDF. Returns '' for scanned images."""
    try:
        with pdfplumber.open(path) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages[:6]]
        return " ".join(pages).strip()
    except Exception:
        return ""


# ── CSV metadata loader ───────────────────────────────────────────────────────

def load_latest_csv(output_dir: Path) -> pd.DataFrame | None:
    files = sorted(
        output_dir.glob("anfragen_*.csv"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    if not files:
        return None
    rows = []
    with open(files[0], encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    if rows:
        log_ok(f"Loaded metadata from {files[0].name}  ({len(rows)} rows)")
    return pd.DataFrame(rows) if rows else None


# ── Mistral API (direct HTTP — no SDK dependency) ────────────────────────────

MISTRAL_EMBED_URL = "https://api.mistral.ai/v1/embeddings"
MISTRAL_CHAT_URL  = "https://api.mistral.ai/v1/chat/completions"


def _mistral_headers(api_key: str) -> dict:
    return {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Authorization": f"Bearer {api_key}",
    }


def embed_texts(api_key: str, texts: list[str]) -> np.ndarray:
    all_embeddings: list[list[float]] = []
    total = len(texts)
    headers = _mistral_headers(api_key)
    for i in range(0, total, EMBED_BATCH):
        batch = [t[:MAX_TEXT_CHARS] for t in texts[i : i + EMBED_BATCH]]
        resp = requests.post(
            MISTRAL_EMBED_URL,
            headers=headers,
            json={"model": EMBED_MODEL, "input": batch},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        # sort by index to keep order consistent
        data.sort(key=lambda x: x["index"])
        all_embeddings.extend(item["embedding"] for item in data)
        log(f"Embedded {min(i + EMBED_BATCH, total)}/{total} ...")
        time.sleep(0.3)
    return np.array(all_embeddings, dtype=np.float32)


# ── UMAP + HDBSCAN ────────────────────────────────────────────────────────────

def reduce_and_cluster(
    embeddings: np.ndarray,
    min_cluster_size: int,
) -> tuple[np.ndarray, np.ndarray]:
    from umap import UMAP  # imported late so startup is fast

    n = len(embeddings)
    n_neighbors = min(15, n - 1)

    # High-dim reduction for clustering quality
    n_cluster_dims = min(15, n - 2)
    log(f"UMAP ({n_cluster_dims}D) for clustering ...")
    umap_cluster = UMAP(
        n_components=n_cluster_dims,
        n_neighbors=n_neighbors,
        min_dist=0.0,
        metric="cosine",
        random_state=42,
        low_memory=False,
    )
    reduced = umap_cluster.fit_transform(embeddings)

    # HDBSCAN
    log("HDBSCAN clustering ...")
    try:
        from sklearn.cluster import HDBSCAN  # sklearn >= 1.3
        labels = HDBSCAN(min_cluster_size=min_cluster_size).fit_predict(reduced)
    except ImportError:
        try:
            import hdbscan as hdbscan_lib
            labels = hdbscan_lib.HDBSCAN(min_cluster_size=min_cluster_size).fit_predict(reduced)
        except ImportError:
            log("HDBSCAN not available — falling back to KMeans (n=8)")
            from sklearn.cluster import KMeans
            k = max(2, min(8, n // max(1, min_cluster_size)))
            labels = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(reduced)

    # 2-D reduction for the plot
    log("UMAP (2D) for visualization ...")
    umap_2d = UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=0.1,
        metric="cosine",
        random_state=42,
    )
    coords = umap_2d.fit_transform(embeddings)
    return coords, labels.astype(int)


# ── Cluster naming via Mistral Large ─────────────────────────────────────────

def name_clusters(
    api_key: str,
    docs: list[dict],
    labels: np.ndarray,
) -> dict[int, str]:
    names: dict[int, str] = {}
    headers = _mistral_headers(api_key)
    for label in sorted(set(labels)):
        if label == -1:
            names[-1] = "Sonstige"
            continue

        sample = [docs[i] for i, l in enumerate(labels) if l == label][:6]
        bullet_list = "\n".join(f"- {d['title']}" for d in sample)

        prompt = (
            "Du analysierst parlamentarische Anfragen (NGO-Business) "
            "aus dem österreichischen Nationalrat.\n\n"
            f"Anfragen aus diesem Cluster:\n{bullet_list}\n\n"
            "Gib diesem Cluster einen prägnanten deutschen Namen (max. 5 Wörter), "
            "der das gemeinsame Thema beschreibt. "
            "Antworte NUR mit dem Namen, ohne Erklärung oder Anführungszeichen."
        )

        resp = requests.post(
            MISTRAL_CHAT_URL,
            headers=headers,
            json={"model": CHAT_MODEL, "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        resp.raise_for_status()
        name = resp.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
        names[label] = name
        log_ok(f"Cluster {label:2d} ({(labels == label).sum()} docs): {name}")
        time.sleep(0.4)

    return names


# ── Plotly interactive figure ─────────────────────────────────────────────────

def build_figure(
    docs: list[dict],
    coords: np.ndarray,
    labels: np.ndarray,
    cluster_names: dict[int, str],
) -> go.Figure:
    fig = go.Figure()

    for idx, label in enumerate(sorted(set(labels))):
        mask = labels == label
        cdocs = [docs[i] for i in range(len(docs)) if labels[i] == label]
        ccoords = coords[mask]

        color = "#666666" if label == -1 else PALETTE[idx % len(PALETTE)]
        cluster_label = cluster_names.get(label, f"Cluster {label}")

        # Hover text
        hover = []
        urls = []
        for d in cdocs:
            title_short = d["title"][:75] + ("…" if len(d["title"]) > 75 else "")
            source_tag = f' <i style="color:#aaa">[{d["text_source"]}]</i>' if d.get("text_source") else ""
            hover.append(
                f"<b>{d['number']}</b>{source_tag}<br>"
                f"{title_short}<br>"
                f"<span style='color:#ccc'>{d['party']} · {d['date']}</span><br>"
                f"<span style='color:#aaa'>Cluster: {cluster_label}</span>"
            )
            urls.append(d.get("detail_url", ""))

        fig.add_trace(go.Scatter(
            x=ccoords[:, 0],
            y=ccoords[:, 1],
            mode="markers",
            name=cluster_label,
            marker=dict(
                size=10,
                color=color,
                opacity=0.85,
                line=dict(width=0.8, color="rgba(255,255,255,0.3)"),
            ),
            text=hover,
            hovertemplate="%{text}<extra></extra>",
            customdata=urls,
        ))

        # Centroid label
        cx, cy = float(ccoords[:, 0].mean()), float(ccoords[:, 1].mean())
        fig.add_annotation(
            x=cx, y=cy,
            text=f"<b>{cluster_label}</b>",
            showarrow=False,
            font=dict(size=11, color=color, family="Arial"),
            bgcolor="rgba(20,20,30,0.75)",
            borderpad=4,
            bordercolor=color,
            borderwidth=1,
        )

    fig.update_layout(
        title=dict(
            text="NGO-Business Anfragen — Themencluster",
            font=dict(size=20, color="#f0f0f0", family="Arial"),
            x=0.5,
        ),
        showlegend=True,
        legend=dict(
            title=dict(text="Cluster", font=dict(color="#ccc")),
            bgcolor="rgba(30,30,45,0.85)",
            bordercolor="#444",
            borderwidth=1,
            font=dict(color="#ddd"),
            itemsizing="constant",
        ),
        plot_bgcolor="#0d0d1a",
        paper_bgcolor="#0d0d1a",
        font=dict(color="#e0e0e0"),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, showline=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, showline=False),
        hovermode="closest",
        hoverlabel=dict(bgcolor="#1e1e2e", bordercolor="#555", font=dict(color="#fff")),
        margin=dict(l=20, r=20, t=70, b=20),
        height=800,
    )

    return fig


def save_html(fig: go.Figure, path: Path) -> None:
    """Write self-contained HTML with click-to-open-URL support."""
    html = pio.to_html(
        fig,
        full_html=True,
        include_plotlyjs=True,
        div_id="anfragen_cluster_map",
        config={"scrollZoom": True, "displayModeBar": True},
    )

    # Inject JavaScript: clicking a point opens its parliament URL in a new tab
    click_js = """
<script>
(function() {
  function attachClickHandler() {
    var el = document.getElementById('anfragen_cluster_map');
    if (!el || !el.on) {
      setTimeout(attachClickHandler, 200);
      return;
    }
    el.on('plotly_click', function(data) {
      if (data.points && data.points.length > 0) {
        var url = data.points[0].customdata;
        if (url && url.startsWith('http')) {
          window.open(url, '_blank');
        }
      }
    });
  }
  document.addEventListener('DOMContentLoaded', attachClickHandler);
})();
</script>
"""
    html = html.replace("</body>", click_js + "\n</body>")
    path.write_text(html, encoding="utf-8")


# ── Cluster CSV export ────────────────────────────────────────────────────────

def save_cluster_csv(docs: list[dict], labels: np.ndarray, cluster_names: dict, path: Path) -> None:
    rows = []
    for doc, label in zip(docs, labels):
        rows.append({
            "cluster_id":   int(label),
            "cluster_name": cluster_names.get(int(label), f"Cluster {label}"),
            "number":       doc["number"],
            "date":         doc["date"],
            "title":        doc["title"],
            "party":        doc["party"],
            "topics":       doc["topics"],
            "text_source":  doc.get("text_source", ""),
            "detail_url":   doc.get("detail_url", ""),
        })
    rows.sort(key=lambda r: (r["cluster_id"], r["date"]))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Cluster NGO Anfragen PDFs with Mistral embeddings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--pdf-dir",          default="output/pdfs", metavar="DIR",
                   help="PDF directory (default: output/pdfs/)")
    p.add_argument("--output-dir",       default="output",      metavar="DIR",
                   help="Output directory (default: output/)")
    p.add_argument("--output-name",      default="clusters",    metavar="NAME",
                   help="Base name for output files, no extension (default: clusters)")
    p.add_argument("--min-cluster-size", default=3, type=int,   metavar="N",
                   help="Min documents per HDBSCAN cluster (default: 3)")
    p.add_argument("--no-pdf-text",      action="store_true",
                   help="Skip PDF text extraction and use titles+topics only")
    p.add_argument("--clean",            action="store_true",
                   help="Delete the output CSV file and exit (safe to open in VS Code after)")
    return p


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    args = build_parser().parse_args()

    if args.clean:
        csv_path = Path(args.output_dir) / f"{args.output_name}.csv"
        if csv_path.exists():
            csv_path.unlink()
            _p(f"  Deleted {csv_path}")
        else:
            _p(f"  Nothing to delete ({csv_path} not found)")
        sys.exit(0)

    api_key = os.getenv("MISTRAL_API_KEY", "")
    if not api_key or api_key == "your_key_here":
        log_err("MISTRAL_API_KEY missing. Add it to your .env file.")
        sys.exit(1)

    pdf_dir    = Path(args.pdf_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Discover PDFs ─────────────────────────────────────────────────────────
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        log_err(f"No PDFs found in {pdf_dir}. Run scraper.py first.")
        sys.exit(1)

    section()
    _p("  NGO-Business Anfragen Cluster")
    section()
    log_ok(f"{len(pdf_files)} PDFs found in {pdf_dir}")

    # ── Load CSV metadata ─────────────────────────────────────────────────────
    meta_df = load_latest_csv(output_dir)

    # ── Build document records ────────────────────────────────────────────────
    section("Extracting document text")
    docs: list[dict] = []
    texts: list[str] = []

    pdf_ok = 0
    pdf_fallback = 0

    for pdf_path in pdf_files:
        # Derive inquiry number from filename  e.g. "5771-J_..." → "5771/J"
        stem_parts = pdf_path.stem.split("_")
        number = stem_parts[0].replace("-", "/") if stem_parts else pdf_path.stem

        # Pull metadata from CSV if available
        meta: dict = {}
        if meta_df is not None:
            row = meta_df[meta_df["number"] == number]
            if not row.empty:
                meta = row.iloc[0].to_dict()

        title      = meta.get("title",      pdf_path.stem)
        party      = meta.get("party",      "")
        date       = meta.get("date",       "")
        detail_url = meta.get("detail_url", "")
        topics     = meta.get("topics",     "")

        # Text for embedding
        pdf_text = "" if args.no_pdf_text else extract_pdf_text(pdf_path)

        if len(pdf_text) > 120:
            embed_text  = pdf_text
            text_source = "pdf"
            pdf_ok += 1
        else:
            embed_text  = f"{title}. {topics}".strip(". ")
            text_source = "title"
            pdf_fallback += 1

        docs.append({
            "number":      number,
            "title":       title,
            "party":       party,
            "date":        date,
            "detail_url":  detail_url,
            "topics":      topics,
            "text_source": text_source,
        })
        texts.append(embed_text)

        status = "[pdf]" if text_source == "pdf" else "[title fallback]"
        log(f"{number}: {title[:55]}  {status}")

    log_ok(f"PDF text extracted: {pdf_ok}  |  title fallback: {pdf_fallback}")

    if len(docs) < 3:
        log_err("Need at least 3 documents to cluster. Download more PDFs first.")
        sys.exit(1)

    # ── Embed ─────────────────────────────────────────────────────────────────
    section(f"Embedding {len(texts)} documents")
    embeddings = embed_texts(api_key, texts)
    log_ok(f"Embedding matrix: {embeddings.shape}")

    # ── Reduce + cluster ──────────────────────────────────────────────────────
    section("UMAP + HDBSCAN")
    coords_2d, labels = reduce_and_cluster(embeddings, args.min_cluster_size)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise    = int((labels == -1).sum())
    log_ok(f"{n_clusters} clusters  |  {n_noise} noise / unassigned points")

    # ── Name clusters ─────────────────────────────────────────────────────────
    section("Naming clusters with Mistral Large")
    cluster_names = name_clusters(api_key, docs, labels)

    # ── Visualize ─────────────────────────────────────────────────────────────
    section("Building interactive visualization")
    fig = build_figure(docs, coords_2d, labels, cluster_names)

    html_path = output_dir / f"{args.output_name}.html"
    csv_path  = output_dir / f"{args.output_name}.csv"

    save_html(fig, html_path)
    log_ok(f"Interactive map  -> {html_path}")

    save_cluster_csv(docs, labels, cluster_names, csv_path)
    log_ok(f"Cluster CSV      -> {csv_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    section("Cluster Summary")
    for label in sorted(set(labels)):
        count = int((labels == label).sum())
        name  = cluster_names.get(label, f"Cluster {label}")
        tag   = " [noise]" if label == -1 else f" [{label:2d}]"
        _p(f"  {tag}  {name:<40} {count:3d} docs")
    section()
    _p(f"\n  Open in your browser: {html_path}\n")


if __name__ == "__main__":
    main()

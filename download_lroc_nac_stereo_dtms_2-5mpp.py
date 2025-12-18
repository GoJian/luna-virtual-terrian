#!/usr/bin/env python3
"""
Download LROC NAC stereo DTMs (ODE product type: SDNDTM) via ODE REST API.

Script to download the south pole data:
> python download_lroc_nac_stereo_dtms_2-5mpp.py --bbox -90 -87 0 360 --mode dtm --limit 1 --dry-run
> python download_lroc_nac_stereo_dtms_2-5mpp.py --bbox -90 -87 0 360 --mode dtm

References:
- ODE REST API manual (live2 endpoint, products query, iipt query)   [oai_citation:4‡Orbital Data Explorer](https://oderest.rsl.wustl.edu/ODE_REST_V2.1.6.pdf)
- ODE LROC SDNDTM description (NAC DTMs + files)                    [oai_citation:5‡Ode](https://ode.rsl.wustl.edu/moon/pagehelp/Content/Missions_Instruments/LRO/LROC/SDR/Intro.htm)
"""

from __future__ import annotations
import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlencode
import requests

ODE_BASE = "https://oderest.rsl.wustl.edu/live2"  # documented in ODE REST manual  [oai_citation:6‡Orbital Data Explorer](https://oderest.rsl.wustl.edu/ODE_REST_V2.1.6.pdf)

def http_get(url: str, timeout: int = 120) -> requests.Response:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r

def safe_filename(name: str) -> str:
    name = name.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

def stream_download(url: str, out_path: Path, chunk_size: int = 2**20) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", "0"))
        downloaded = 0
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = 100.0 * downloaded / total
                    sys.stdout.write(f"\r  {out_path.name}: {pct:6.2f}%")
                    sys.stdout.flush()
        tmp_path.replace(out_path)
    sys.stdout.write("\n")

def iipt_find_lroc_sndtm() -> dict:
    """
    Use ODE 'iipt' query to find valid IHID/IID/PT tokens for Moon LRO LROC SDNDTM.
    The ODE REST manual recommends iipt for discovering valid IIPT sets.  [oai_citation:7‡Orbital Data Explorer](https://oderest.rsl.wustl.edu/ODE_REST_V2.1.6.pdf)
    """
    params = {
        "query": "iipt",
        "odemetadb": "moon",
        "output": "JSON",
    }
    url = f"{ODE_BASE}?{urlencode(params)}"
    data = http_get(url).json()

    # Response structure can evolve; we search loosely
    # Expect something like: IIPTSets -> IIPTSet -> {IHID, IID, PT, ...}
    sets = []
    def walk(obj):
        if isinstance(obj, dict):
            if "IHID" in obj and "IID" in obj and "PT" in obj:
                sets.append(obj)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
    walk(data)

    # Find LRO/LROC + SDNDTM
    lroc_sets = [s for s in sets if str(s.get("IHID", "")).upper() == "LRO" and str(s.get("IID", "")).upper() == "LROC"]
    print(f"Found LRO/LROC IIPT sets: {[s.get('PT') for s in lroc_sets]}")
    for s in lroc_sets:
        if str(s.get("PT", "")).upper() == "SDNDTM":
            return {"ihid": "LRO", "iid": "LROC", "pt": "SDNDTM"}

    raise RuntimeError("Could not locate LRO/LROC SDNDTM in ODE iipt response. "
                       "Try printing the iipt JSON and search for LROC + DTM tokens.")

def query_products_bbox(ihid: str, iid: str, pt: str,
                        minlat: float, maxlat: float,
                        westernlon: float, easternlon: float,
                        loc: str = "f",
                        limit: int = 1000) -> dict:
    """
    Query products intersecting a bounding box. The ODE REST manual shows
    minlat/maxlat/westernlon/easternlon with loc modes (e.g., footprint intersects).  [oai_citation:8‡Orbital Data Explorer](https://oderest.rsl.wustl.edu/ODE_REST_V2.1.6.pdf)
    """
    params = {
        "query": "products",
        "target": "moon",
        "ihid": ihid,
        "iid": iid,
        "pt": pt,
        "minlat": f"{minlat}",
        "maxlat": f"{maxlat}",
        "westernlon": f"{westernlon}",
        "easternlon": f"{easternlon}",
        "loc": loc,          # footprint relationship mode; examples in manual  [oai_citation:9‡Orbital Data Explorer](https://oderest.rsl.wustl.edu/ODE_REST_V2.1.6.pdf)
        "results": "opm",    # request product metadata including files; manual examples show results=opm/c etc  [oai_citation:10‡Orbital Data Explorer](https://oderest.rsl.wustl.edu/ODE_REST_V2.1.6.pdf)
        "output": "JSON",
        "limit": str(limit),
    }
    url = f"{ODE_BASE}?{urlencode(params)}"
    return http_get(url).json()

def fetch_product_files_from_url(files_url: str) -> list[dict]:
    """
    Fetch the product files page and extract file URLs.
    The page is an ASPX page with links to files.
    """
    print(f"Fetching {files_url}")
    try:
        r = http_get(files_url)
        html = r.text
    except Exception as e:
        print(f"Failed to fetch {files_url}: {e}")
        return []

    # Parse links; look for <a href="..."> with file extensions
    import re
    links = re.findall(r'<a[^>]*href="([^"]*)"[^>]*>([^<]*)</a>', html, re.IGNORECASE)
    files = []
    for url, text in links:
        url = url.strip()
        text = text.strip()
        if not url or url in ('#', '/', '../'):
            continue
        # Make absolute URL if relative
        if not url.startswith('http'):
            from urllib.parse import urljoin
            url = urljoin(files_url, url)
        # Guess filename from URL or text
        filename = text.split()[0] if text else os.path.basename(url.split('?')[0])
        # Filter for likely data files
        if any(filename.lower().endswith(ext) for ext in ['.tif', '.tiff', '.xml', '.lbl', '.jpg', '.png']):
            files.append({"url": url, "filename": filename, "type": "data"})
    return files


def extract_product_file_urls(products_json: dict) -> list[tuple[dict, list[dict]]]:
    """
    Extract per-product file URLs. For SDNDTM, files are not in the products JSON,
    but there's a FilesURL pointing to a page listing the files.
    """
    products = []

    def walk(obj):
        if isinstance(obj, dict):
            # A "Product" node
            if "pdsid" in obj or "odeid" in obj or "ode_id" in obj:
                products.append(obj)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(products_json)

    extracted = []
    for p in products:
        pid = p.get("pdsid") or p.get("Pdsid") or p.get("PDSID") or p.get("ProductId") or p.get("Product_id") or p.get("ode_id")
        odeid = p.get("odeid") or p.get("ODEid") or p.get("ODEID") or p.get("ode_id")
        files_url = p.get("External_url", "").rsplit('/', 1)[0] + '/' if p.get("External_url") else None
        files = []
        if files_url:
            files = fetch_product_files_from_url(files_url)
        extracted.append((p, files))

    return extracted

def choose_files(files: list[dict], mode: str) -> list[dict]:
    """
    mode:
      - "dtm": main DTM GeoTIFF only (and detached label if present)
      - "all_tif": all GeoTIFFs (DTM + slope/shade/etc)
      - "all": everything
    """
    if mode == "all":
        return files

    if mode == "all_tif":
        return [f for f in files if f["filename"].lower().endswith(".tif")]

    # default: dtm
    # Heuristic: main DTM is usually the only .TIF without suffix like _SHADE/_SLOPE/_CLRSHADE
    tif = [f for f in files if f["filename"].lower().endswith(".tif")]
    main = []
    for f in tif:
        name = f["filename"].upper()
        if any(s in name for s in ["_SHADE", "_SLOPE", "_CLRSHADE", "_LEGEND"]):
            continue
        main.append(f)

    # Also grab label files if present (.LBL, .XML)
    labels = [f for f in files if f["filename"].lower().endswith((".lbl", ".xml"))]

    # If heuristic fails, fall back to all .tif
    if not main and tif:
        main = tif

    # Keep unique by filename
    picked = {f["filename"]: f for f in (main + labels)}
    return list(picked.values())

def main():
    ap = argparse.ArgumentParser(description="Download LROC NAC stereo DTMs (SDNDTM) via ODE REST.")
    ap.add_argument("--bbox", nargs=4, type=float, metavar=("MINLAT", "MAXLAT", "WESTLON", "EASTLON"),
                    required=True,
                    help="Bounding box in degrees. Longitudes are typically 0–360 in ODE.")
    ap.add_argument("--out", type=Path, default=Path("data/NAC_stereo_DTM_2-5mpp"),
                    help="Output directory.")
    ap.add_argument("--mode", type=str, choices=["dtm", "all_tif", "all"], default="dtm",
                    help="Which files to download per product.")
    ap.add_argument("--loc", type=str, default="f",
                    help="ODE footprint relation mode (default 'f' = footprint intersects; see ODE REST manual examples).")
    ap.add_argument("--limit", type=int, default=500, help="Max products to return.")
    ap.add_argument("--dry-run", action="store_true", help="List products/files but do not download.")
    args = ap.parse_args()

    minlat, maxlat, westlon, eastlon = args.bbox
    out_dir = args.out

    # 1) Discover valid tokens (IHID/IID/PT)
    tokens = iipt_find_lroc_sndtm()
    ihid, iid, pt = tokens["ihid"], tokens["iid"], tokens["pt"]
    print(f"Using IIPT: ihid={ihid} iid={iid} pt={pt}")

    # 2) Query products in bbox
    prod_json = query_products_bbox(
        ihid=ihid, iid=iid, pt=pt,
        minlat=minlat, maxlat=maxlat,
        westernlon=westlon, easternlon=eastlon,
        loc=args.loc, limit=args.limit
    )

    # Save metadata for reproducibility
    out_dir.mkdir(parents=True, exist_ok=True)
    meta_path = out_dir / "ode_query_response.json"
    meta_path.write_text(json.dumps(prod_json, indent=2))
    print(f"Saved query JSON: {meta_path}")

    # 3) Extract file URLs
    product_files = extract_product_file_urls(prod_json)

    # Filter products by bbox
    filtered = []
    for p, files in product_files:
        if not files:
            continue
        lat = float(p.get('Center_latitude', 0))
        lon = float(p.get('Center_longitude', 0))
        if minlat <= lat <= maxlat and westlon <= lon <= eastlon:
            filtered.append((p, files))

    print(f"Found {len(filtered)} products with downloadable files.")

    if args.dry_run:
        for p, files in filtered[:10]:
            pdsid = p.get("pdsid") or p.get("Pdsid") or p.get("PDSID") or p.get("ProductId") or p.get("Product_id") or p.get("ode_id")
            odeid = p.get("odeid") or p.get("ODEid") or p.get("ODEID") or p.get("ode_id")
            print(f"\nProduct pdsid={pdsid} odeid={odeid}")
            picked = choose_files(files, args.mode)
            for f in picked:
                print(f"  - {f['filename']}  {f['url']}")
        print("\nDry run complete.")
        return

    # 4) Download selected files per product
    for p, files in filtered:
        pdsid = p.get("pdsid") or p.get("Pdsid") or p.get("PDSID") or p.get("ProductId") or p.get("Product_id") or p.get("ode_id")
        pid = safe_filename(str(pdsid or "unknown_product"))
        prod_dir = out_dir / pid
        picked = choose_files(files, args.mode)

        print(f"\nDownloading {len(picked)} files for {pid}")
        for f in picked:
            url = f["url"]
            fn = safe_filename(f["filename"])
            out_path = prod_dir / fn
            if out_path.exists() and out_path.stat().st_size > 0:
                print(f"  exists: {out_path}")
                continue
            print(f"  url: {url}")
            stream_download(url, out_path)

if __name__ == "__main__":
    main()
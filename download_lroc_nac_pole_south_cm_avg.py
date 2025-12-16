#!/usr/bin/env python3
"""
Download LROC RDR product tiles from the NAC_POLE_SOUTH_CM_AVG directory listing.

Source index (Apache listing):
https://lroc.im-ldi.com/data/LRO-L-LROC-5-RDR-V1.0/LROLRC_2001/EXTRAS/BROWSE/NAC_POLE/NAC_POLE_SOUTH_CM_AVG

The HTML page itself indicates the canonical base URL for downloads is the pds host:
https://pds.lroc.im-ldi.com/data/.../EXTRAS/BROWSE/NAC_POLE/NAC_POLE_SOUTH_CM_AVG/
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from tqdm import tqdm

# -----------------------------
# Configuration / constants
# -----------------------------

# Index page you can open in a browser (easy to inspect):
INDEX_URL = (
    "https://lroc.im-ldi.com/data/LRO-L-LROC-5-RDR-V1.0/"
    "LROLRC_2001/EXTRAS/BROWSE/NAC_POLE/NAC_POLE_SOUTH_CM_AVG"
)

# Canonical download base URL shown at top of the index page (line 0 in the listing):
BASE_URL = (
    "https://pds.lroc.im-ldi.com/data/LRO-L-LROC-5-RDR-V1.0/"
    "LROLRC_2001/EXTRAS/BROWSE/NAC_POLE/NAC_POLE_SOUTH_CM_AVG/"
)

# Conservative timeout: (connect, read)
TIMEOUT = (30, 300)


@dataclass(frozen=True)
class RemoteFile:
    name: str
    url: str


def verify_files(session: requests.Session, filenames: List[str], out_dir: Path):
    print("Verifying existing files...")
    complete = 0
    incomplete = 0
    missing = 0
    for name in filenames:
        out_path = out_dir / name
        url = urljoin(BASE_URL, name)
        remote_size = remote_head(session, url)
        if not out_path.exists():
            print(f"Missing: {name}")
            missing += 1
            continue
        local_size = out_path.stat().st_size
        if remote_size is not None and local_size == remote_size:
            complete += 1
        else:
            print(f"Incomplete or size mismatch: {name} (local: {local_size}, remote: {remote_size})")
            incomplete += 1
    print(f"Verification complete: {complete} complete, {incomplete} incomplete, {missing} missing")


def fetch_index_html(session: requests.Session) -> str:
    max_retries = 6
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(INDEX_URL, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt == max_retries:
                raise
            sleep_s = min(2 ** attempt, 30)
            print(f"[retry {attempt}/{max_retries}] Fetching index: {e} (sleep {sleep_s}s)")
            time.sleep(sleep_s)


def parse_index_for_filenames(html: str) -> List[str]:
    """
    Parse the Apache-style listing HTML and extract file names from <a href="...">.
    """
    # Typical listing has <a href="FILENAME">FILENAME</a>
    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    names = []
    for h in hrefs:
        # filter out Parent Directory and subpaths
        if h in ("../", "/"):
            continue
        # In Apache listings, href is the filename
        if h.endswith("/"):
            continue
        # Avoid query strings
        h = h.split("?", 1)[0]
        # Some listings include absolute paths; keep basename
        name = os.path.basename(h)
        if name:
            names.append(name)
    # Deduplicate but keep stable order
    seen = set()
    out = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def filter_files(
    filenames: Iterable[str],
    include_ext: Tuple[str, ...],
    include_masks: bool,
    include_pyramids: bool,
    include_xml: bool,
    include_browse_png: bool,
) -> List[str]:
    """
    Decide which files to download based on extension and suffix patterns.
    """
    include_ext = tuple(e.lower().lstrip(".") for e in include_ext)

    keep: List[str] = []
    for fn in filenames:
        lower = fn.lower()

        # Always skip tiny icons / non-data stuff if any appear
        if lower.endswith((".gif", ".ico")):
            continue

        # Browse PNGs are explicit in name: *.BROWSE.PNG
        if lower.endswith(".browse.png"):
            if include_browse_png:
                keep.append(fn)
            continue

        # Masks: *.MASK.TIF
        if lower.endswith(".mask.tif"):
            if include_masks:
                keep.append(fn)
            continue

        # Pyramids: *.PYR.TIF (these are HUGE; often not needed for your pipeline)
        if lower.endswith(".pyr.tif"):
            if include_pyramids:
                keep.append(fn)
            continue

        # XML sidecars
        if lower.endswith(".xml"):
            if include_xml:
                keep.append(fn)
            continue

        # Main GeoTIFF tiles: *.TIF
        # Note: this will also match MASK/PYR, but those were already handled above.
        ext = lower.rsplit(".", 1)[-1] if "." in lower else ""
        if ext in include_ext:
            keep.append(fn)

    return keep


def remote_head(session: requests.Session, url: str) -> Optional[int]:
    """
    Return Content-Length if available.
    """
    try:
        r = session.head(url, allow_redirects=True, timeout=TIMEOUT)
        if r.status_code >= 400:
            return None
        cl = r.headers.get("Content-Length")
        return int(cl) if cl is not None else None
    except Exception:
        return None


def download_with_resume(
    session: requests.Session,
    url: str,
    out_path: Path,
    chunk_size: int = 8 * 1024 * 1024,
    max_retries: int = 6,
) -> None:
    """
    HTTP download with resume support via Range requests.
    Writes to out_path (in-place). Creates parent dirs.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Determine remote size (best-effort)
    remote_size = remote_head(session, url)

    # Determine local resume point
    local_size = out_path.stat().st_size if out_path.exists() else 0

    # If already complete, skip
    if remote_size is not None and local_size == remote_size:
        return

    headers = {}
    mode = "ab" if local_size > 0 else "wb"
    if local_size > 0:
        headers["Range"] = f"bytes={local_size}-"

    for attempt in range(1, max_retries + 1):
        try:
            with session.get(url, stream=True, headers=headers, timeout=TIMEOUT) as r:
                # 200 = full content, 206 = partial content
                if r.status_code not in (200, 206):
                    r.raise_for_status()

                pbar = None
                if remote_size is not None:
                    pbar = tqdm(total=remote_size, initial=local_size, unit='B', unit_scale=True, desc=out_path.name)
                with open(out_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))
                if pbar:
                    pbar.close()

            # Post-check size if known
            if remote_size is not None:
                final_size = out_path.stat().st_size
                if final_size != remote_size:
                    print(f"Warning: Size mismatch for {out_path.name}: got {final_size}, expected {remote_size}. Keeping the file.")

            return  # success

        except Exception as e:
            if attempt == max_retries:
                raise
            # Backoff and try again
            sleep_s = min(2 ** attempt, 30)
            print(f"[retry {attempt}/{max_retries}] {out_path.name}: {e} (sleep {sleep_s}s)")
            time.sleep(sleep_s)
            # Update resume headers for next attempt
            local_size = out_path.stat().st_size if out_path.exists() else 0
            headers["Range"] = f"bytes={local_size}-" if local_size > 0 else ""
            mode = "ab" if local_size > 0 else "wb"


def main():
    ap = argparse.ArgumentParser(
        description="Download LROC NAC South Pole Controlled Average Mosaic tiles (NAC_POLE_SOUTH_CM_AVG)."
    )
    ap.add_argument("--out", type=Path, default=Path("data/NAC_POLE_SOUTH_CM_AVG"), help="Output directory")
    ap.add_argument(
        "--ext",
        nargs="+",
        default=["tif"],
        help="Main extensions to include (default: tif). Example: --ext tif",
    )
    ap.add_argument("--include-browse-png", action="store_true", help="Also download *.BROWSE.PNG preview images")
    ap.add_argument("--include-masks", action="store_true", help="Also download *.MASK.TIF files")
    ap.add_argument("--include-pyramids", action="store_true", help="Also download *.PYR.TIF files (VERY large)")
    ap.add_argument("--include-xml", action="store_true", help="Also download *.xml label files")
    ap.add_argument("--max-workers", type=int, default=1, help="Parallel download workers (default: 1)")
    ap.add_argument("--dry-run", action="store_true", help="List what would be downloaded, but don't download")
    ap.add_argument("--verify", action="store_true", help="Verify existing files against remote sizes")
    args = ap.parse_args()

    with requests.Session() as session:
        html = fetch_index_html(session)
        all_names = parse_index_for_filenames(html)

        selected = filter_files(
            all_names,
            include_ext=tuple(args.ext),
            include_masks=args.include_masks,
            include_pyramids=args.include_pyramids,
            include_xml=args.include_xml,
            include_browse_png=args.include_browse_png,
        )

        if args.verify:
            verify_files(session, selected, args.out)
            return

        # Convert to full URLs
        remotes = [RemoteFile(name=n, url=urljoin(BASE_URL, n)) for n in selected]

        print(f"Index: {INDEX_URL}")
        print(f"Base : {BASE_URL}")
        print(f"Found {len(all_names)} files; selected {len(remotes)} files.")

        # Check for existing files
        existing = []
        for rf in remotes:
            out_path = args.out / rf.name
            if out_path.exists():
                existing.append(rf)
                print(f"Already exists: {rf.name}")
        remotes = [rf for rf in remotes if rf not in existing]
        print(f"Skipping {len(existing)} existing files; downloading {len(remotes)} files.")

        if args.dry_run:
            for rf in remotes[:200]:
                print(rf.url)
            if len(remotes) > 200:
                print(f"... ({len(remotes)-200} more)")
            return

        def job(rf: RemoteFile):
            out_path = args.out / rf.name
            download_with_resume(session, rf.url, out_path)
            return rf.name

        # Note: requests.Session is not strictly thread-safe for heavy use.
        # For maximum robustness, create a new Session per thread:
        def threaded_job(rf: RemoteFile):
            with requests.Session() as s:
                out_path = args.out / rf.name
                download_with_resume(s, rf.url, out_path)
                return rf.name

        with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [ex.submit(threaded_job, rf) for rf in remotes]
            done = 0
            for fut in cf.as_completed(futures):
                name = fut.result()
                done += 1
                print(f"[{done}/{len(remotes)}] downloaded: {name}")

    print("Done.")


if __name__ == "__main__":
    main()
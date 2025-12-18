#!/usr/bin/env python3
"""
Download LOLA 20mpp product tiles.

Files are hardcoded as the directory does not provide an Apache-style listing.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin

import requests
from tqdm import tqdm

# -----------------------------
# Configuration / constants
# -----------------------------

# Base URL for downloads:
BASE_URL = "https://pgda.gsfc.nasa.gov/data/LOLA_20mpp/"

# Default files to download
FILES_DEFAULT = [
    "LDEM_80S_20MPP_ADJ.TIF",          # elevation (meters)
    "LDEC_80S_20MPP_ADJ.TIF",          # counts
    "LDEM_80S_20MPP_ADJ_ERR.TIF",      # elevation error
    "LDEM_80S_20MPP_ADJ_EFFRES.TIF",   # effective resolution (meters)
    "LDSM_80S_20MPP_ADJ.TIF",          # slope (degrees)
]

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
        # For XML files, server size reporting may be unreliable; assume complete if exists and >1KB
        if name.lower().endswith('.xml'):
            if local_size > 1024:
                complete += 1
            else:
                print(f"Incomplete: {name} (local: {local_size}, too small for XML)")
                incomplete += 1
        elif remote_size is not None and local_size == remote_size:
            complete += 1
        else:
            print(f"Incomplete or size mismatch: {name} (local: {local_size}, remote: {remote_size})")
            incomplete += 1
    print(f"Verification complete: {complete} complete, {incomplete} incomplete, {missing} missing")


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
        description="Download LOLA 20mpp product tiles."
    )
    ap.add_argument("--out", type=Path, default=Path("data/DEM_LOLA_20mpp"), help="Output directory")
    ap.add_argument("--files", nargs="*", default=FILES_DEFAULT,
                    help="File list to download (default: standard LOLA 20mpp files).")
    ap.add_argument("--max-workers", type=int, default=1, help="Parallel download workers (default: 1)")
    ap.add_argument("--dry-run", action="store_true", help="List what would be downloaded, but don't download")
    ap.add_argument("--verify", action="store_true", help="Verify existing files against remote sizes")
    args = ap.parse_args()

    selected = args.files

    with requests.Session() as session:
        if args.verify:
            verify_files(session, selected, args.out)
            return

        # Convert to full URLs
        remotes = [RemoteFile(name=n, url=urljoin(BASE_URL, n)) for n in selected]

        print(f"Base : {BASE_URL}")
        print(f"Selected {len(remotes)} files.")

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
            for rf in remotes:
                print(rf.url)
            return

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
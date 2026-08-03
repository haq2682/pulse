"""
Build-time NLTK corpus download for mapping/algorithms/nltk_mapping.py,
which needs these packages at import time. Baked into the image here
(see the Dockerfile) instead of downloaded at request time, since
mapping/map.py's Spark driver runs as a subprocess of this same
container - a runtime download makes the entire mapping pipeline depend
on live internet access, which was verified to hang/crawl indefinitely
under this environment's network conditions.

Uses nltk.download() with raise_on_error=True rather than the interactive
`python3 -m nltk.downloader` CLI - verified live that on a corrupted
download (integrity check failure, a real risk on a flaky network) the
CLI prompts interactively ("Retry? [n/y/e]"), which has no stdin in a
Docker build and crashes with EOFError instead of retrying. This retries
each package a few times on any failure instead.
"""
import sys
import time

import nltk

DOWNLOAD_DIR = "/usr/local/share/nltk_data"
PACKAGES = ["punkt", "punkt_tab", "stopwords", "wordnet", "averaged_perceptron_tagger"]
MAX_ATTEMPTS = 6
RETRY_DELAY_SECONDS = 5


def download_with_retry(package: str) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            nltk.download(package, download_dir=DOWNLOAD_DIR, raise_on_error=True)
            return
        except Exception as exc:
            print(f"[retry] {package} attempt {attempt} failed: {exc}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)
    sys.exit(f"giving up on {package} after {MAX_ATTEMPTS} attempts")


if __name__ == "__main__":
    for package in PACKAGES:
        download_with_retry(package)

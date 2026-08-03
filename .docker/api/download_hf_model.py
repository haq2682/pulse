"""
Build-time download of the sentence-transformers model used by
mapping/algorithms/roberta_mapping.py at import time
(SentenceTransformer("all-MiniLM-L12-v2")). Baked into the image here for
the same reason as download_nltk.py in this same directory: mapping/map.py's
Spark driver runs as a subprocess of this container, so a runtime download
makes the whole mapping pipeline depend on live internet access - verified
live that this specific model's weights file (model.safetensors) stalled
indefinitely partway through under this environment's network conditions.

Relies on HF_HOME (set in the Dockerfile before this runs) pointing at a
world-readable location, so the non-root appuser this container runs as
finds the same cached model with no re-download.
"""
import sys
import time

from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L12-v2"
MAX_ATTEMPTS = 6
RETRY_DELAY_SECONDS = 5

for attempt in range(1, MAX_ATTEMPTS + 1):
    try:
        SentenceTransformer(MODEL_NAME)
        break
    except Exception as exc:
        print(f"[retry] {MODEL_NAME} attempt {attempt} failed: {exc}")
        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_DELAY_SECONDS)
        else:
            sys.exit(f"giving up on {MODEL_NAME} after {MAX_ATTEMPTS} attempts")

"""
Safety net for the NLTK download step (see download_nltk.py). nltk's own
downloader is supposed to unzip each package after downloading it, but
verified live that it silently left wordnet.zip un-extracted (present on
disk at the correct size, just never unzipped) while other packages in
the same run extracted fine - a real, reproducible downloader quirk, not
a corrupt/partial download. Extracts anything left as a bare .zip instead
of a real resource directory.
"""
import pathlib
import zipfile

ROOT = pathlib.Path("/usr/local/share/nltk_data")

for zip_path in ROOT.rglob("*.zip"):
    extracted_marker = zip_path.with_suffix("")
    if not extracted_marker.exists():
        print(f"extracting {zip_path}")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(zip_path.parent)

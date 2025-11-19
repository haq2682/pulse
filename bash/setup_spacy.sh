#!/bin/bash
set -e

echo "Downloading spaCy model (en_core_web_md)..."
python -m spacy download en_core_web_md

echo "Verifying spaCy model..."
python -c "import spacy; nlp = spacy.load('en_core_web_md'); print('✓ Model loaded successfully')"
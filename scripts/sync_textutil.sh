#!/usr/bin/env bash
# Keep the query tokenizer identical to the index tokenizer.
cd "$(dirname "$0")/.." && cp jobs/textutil.py search/textutil.py && echo "synced"

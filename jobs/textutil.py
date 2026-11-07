"""Tokenizer shared by the Spark indexer (Nodes 2/3) and the search service (Node 4).

IMPORTANT: the index and the query MUST be tokenized identically, otherwise
queries silently miss. search/textutil.py is a byte-for-byte copy of this file
(scripts/sync_textutil.sh copies it). Never edit one without the other.
"""
import re

TOKEN_RE = re.compile(r"[a-z0-9]+")

STOPWORDS = frozenset("""
a about above after again against all am an and any are as at be because been
before being below between both but by can could did do does doing down during
each few for from further had has have having he her here hers herself him
himself his how i if in into is it its itself just me more most my myself no nor
not now of off on once only or other our ours ourselves out over own same she
should so some such than that the their theirs them themselves then there these
they this those through to too under until up very was we were what when where
which while who whom why will with would you your yours yourself yourselves
also may might must shall s t
""".split())


def tokenize(text):
    """lowercase -> alphanumeric runs -> drop stopwords and 1-char tokens."""
    if not text:
        return []
    return [t for t in TOKEN_RE.findall(text.lower())
            if len(t) > 1 and t not in STOPWORDS]

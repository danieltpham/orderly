# alias_cleaning.py
# Pipeline:
# 1) normalize
# 2) collapse typos by nearest frequent neighbor (edit distance) -> rep_map
# 3) transform aliases by replacing tokens with representatives
# 4) select most frequent canonical tokens
# 5) rank ORIGINAL aliases by similarity to those tokens (order-insensitive, tie-break by frequency)
# 6) canonicalize the top-3 by applying rep_map

import re
from collections import Counter
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from rapidfuzz import fuzz
from rapidfuzz.distance import Levenshtein

# ---------------- Core utils ----------------

_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)
_WS_RE = re.compile(r"\s+")

def _normalize(s: str) -> str:
    """Normalize string: lowercase, remove punctuation, filter stop words."""
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return " ".join(t for t in s.split() if t and t not in ENGLISH_STOP_WORDS)

def _norm_tokens(s: str):
    """Get normalized tokens that are alphabetic only."""
    return [t for t in _normalize(s).split() if t.isalpha()]

# ---------------- Typo collapse (edit-distance) ----------------

def build_rep_map_typo_collapse(token_lists, max_edit_distance=2, min_rep_len=4):
    """
    Map each token to the most frequent vocabulary neighbor within Levenshtein
    distance <= max_edit_distance. Representatives must be at least min_rep_len
    unless it's the exact same token.

    Effect: 'keybord', 'kayboard' -> 'keyboard'; 'keyboard' stays 'keyboard'.
    """
    counts = Counter(t for lst in token_lists for t in lst)
    vocab = list(counts.keys())
    if not vocab:
        return {}

    # Sort potential representatives by desirability: higher freq, then longer, then lexicographic
    reps_sorted = sorted(vocab, key=lambda x: (counts[x], len(x), x), reverse=True)

    rep_for = {}
    for t in vocab:
        best = t
        # Prefer the first close, frequent-enough representative
        for r in reps_sorted:
            if r == t:
                best = r
                break
            if len(r) < min_rep_len:
                continue
            if Levenshtein.distance(t, r) <= max_edit_distance:
                best = r
                break
        rep_for[t] = best
    return rep_for

def _apply_rep_map(token_lists, rep_map):
    """Replace each token with its representative; unknown tokens remain unchanged."""
    return [[rep_map.get(t, t) for t in lst] for lst in token_lists]

# ---------------- Canonical tokens & ranking ----------------

def most_frequent_canonical_tokens(transformed_token_lists, top_m=5, use_presence=True):
    """
    Select the most frequent canonical tokens after variant merge.
    - use_presence=True counts presence per alias (set-based).
    """
    if use_presence:
        counter = Counter(t for lst in transformed_token_lists for t in set(lst))
    else:
        counter = Counter(t for lst in transformed_token_lists for t in lst)
    if not counter:
        return []
    # sort by frequency desc, then length desc, then lexicographic
    items = sorted(counter.items(), key=lambda kv: (-kv[1], -len(kv[0]), kv[0]))
    return [t for t, _ in items[:top_m]]

def _alias_to_transformed_string(alias, rep_map):
    """Normalize alias to tokens and replace each with rep; return joined string."""
    toks = _norm_tokens(alias)
    ttoks = [rep_map.get(t, t) for t in toks]
    return " ".join(ttoks)

def rank_by_tokens(aliases, target_tokens, k=3):
    """
    Rank ORIGINAL aliases by similarity to target token string (order-insensitive).
    Tie-break by alias frequency in the input list.
    """
    if not target_tokens:
        return []
    target = " ".join(target_tokens)
    target_norm = _normalize(target)
    freq = Counter(aliases)
    scored = []
    for a in aliases:
        if not a or not a.strip():
            continue
        a_norm = _normalize(a)
        # token-set is robust to word order and duplicates
        score = fuzz.token_sort_ratio(a_norm, target_norm)
        scored.append((a, score, freq[a]))
    scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return scored[:k]

# ---------------- End-to-end pipeline ----------------

def transform_aliases_with_canonical_tokens(
    aliases,
    *,
    max_edit_distance=1,
    min_rep_len=4,
    top_m_canonical_tokens=5,
    top_k_original=3,
):
    """
    1) Build rep_map via typo collapse (edit-distance)
    2) Transform aliases by replacing tokens via rep_map
    3) Pick most frequent canonical tokens (top M)
    4) Rank ORIGINAL aliases vs those tokens (order-insensitive, tie-break by freq)
    5) Canonicalize the top-K by applying rep_map
    """
    al = [a for a in aliases if a and a.strip()]
    if not al:
        return {
            "rep_map": {},
            "transformed_aliases": [],
            "canonical_tokens": [],
            "top_k_original": [],
            "top_k_canonicalized": [],
            "params": {},
        }

    token_lists = [_norm_tokens(a) for a in al]
    rep_map = build_rep_map_typo_collapse(token_lists, max_edit_distance=max_edit_distance, min_rep_len=min_rep_len)
    transformed_lists = _apply_rep_map(token_lists, rep_map)
    transformed_aliases = [" ".join(lst) for lst in transformed_lists]

    canonical_tokens = most_frequent_canonical_tokens(
        transformed_lists, top_m=top_m_canonical_tokens, use_presence=True
    )

    top_original = rank_by_tokens(al, canonical_tokens, k=top_k_original)

    # Canonicalize the selected top original aliases
    top_canonicalized = [
        {
            "alias": a,
            "alias_canonicalized": _alias_to_transformed_string(a, rep_map),
            "score": s,
            "freq": f,
        }
        for (a, s, f) in top_original
    ]

    return {
        "rep_map": rep_map,
        "transformed_aliases": transformed_aliases,
        "canonical_tokens": canonical_tokens,
        "top_k_original": [{"alias": a, "score": s, "freq": f} for (a, s, f) in top_original],
        "top_k_canonicalized": top_canonicalized,
        "params": {
            "max_edit_distance": max_edit_distance,
            "min_rep_len": min_rep_len,
            "top_m_canonical_tokens": top_m_canonical_tokens,
            "top_k_original": top_k_original,
        },
    }

# ---------------- Demo ----------------
if __name__ == "__main__":
    aliases = [
        "tf wireless keybord black",
        "wireless kb black",
        "wireless keybord black tf",
        "wireless keyboard black",
        "tf wireless kb black",
        "techflow keybord wireles black",
        "techflow wireless keyboard black",
        "techflow wireless keyboard",
        "wordpress wireless kayboard",
        "brand new wireless keyboard black",
        "wireless keyboard black techflow",
        "wireless keybord black",
        "wireless keyboard",
        "wireless keyboard - black",
        "wireless kb",
        "black (tf)"
    ]

    result = transform_aliases_with_canonical_tokens(
        aliases,
        max_edit_distance=1,
        min_rep_len=4,
        top_m_canonical_tokens=5,
        top_k_original=3,
    )

    print("\nRep map (sample):", dict(list(result["rep_map"].items())[:12]))
    print("\nTransformed (alias -> transformed):")
    for a, t in zip(aliases, result["transformed_aliases"]):
        print(f"  {a!r} -> {t!r}")

    print("\nMost frequent canonical tokens:", result["canonical_tokens"])

    print("\nTop-3 ORIGINAL by similarity to canonical tokens:")
    for item in result["top_k_original"]:
        print(f"  - {item['alias']} (score: {item['score']}, freq: {item['freq']})")

    print("\nTop-3 canonicalized:")
    for item in result["top_k_canonicalized"]:
        print(f"  - {item['alias']} -> {item['alias_canonicalized']} (score: {item['score']}, freq: {item['freq']})")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline tests of the pure award helpers of Process 44 (TMDB-MOVIE-PREPROCESS-071).

    uv run --no-project python test-071-award-helpers.py

No database, no network. Pins the rules of README, Process 44:

- f_awardwinrows(): one row per (statement, year); the year is P585, else the year of the P805
  ceremony, NULL outside 1000 to next year; the work is kept only when nothing is ambiguous
  (several years and several works: no work; one year and several works: the lowest id);
- f_awardwinentities(): the flat qualifier lines of the reader folded into entities and statements;
- f_awardfamilypick(): the family of a category with several candidates is the one with the most
  member categories, then the route P31 > P361 > P527, then the lowest Q number.

The modules the helpers file imports and this machine may not have are replaced by empty ones.
Exit code 1 when a case fails.
"""
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for name in ("pymysql", "pymysql.cursors", "requests", "pandas", "psutil", "citizenphil"):
    try:
        __import__(name)
    except Exception:
        sys.modules[name] = types.ModuleType(name)
import tmdb_preprocess_helpers as h

W = h.f_awardwinrows
cy = {"C82": 1982, "C83": 1983, "C82b": 1982, "Cx": None}
wm = {"W1": 11816, "W2": 500, "W3": None}
ws = {"W1": None, "W2": None, "W3": 77}
def row(y, c=None, m=None, s=None):
    return {"AWARD_YEAR": y, "ID_CEREMONY_WIKIDATA": c, "ID_MOVIE_FOR": m, "ID_SERIE_FOR": s}
cases = [
 ("one year one work", W({1982}, {"W1"}, set(), cy, wm, ws, 2027), [row(1982, None, 11816)]),
 ("no qualifier", W(set(), set(), set(), cy, wm, ws, 2027), [row(None)]),
 ("two years no work", W({1990, 1991}, set(), set(), cy, wm, ws, 2027), [row(1990), row(1991)]),
 ("two years one work", W({1990, 1991}, {"W2"}, set(), cy, wm, ws, 2027), [row(1990, None, 500), row(1991, None, 500)]),
 ("two years two works", W({1990, 1991}, {"W1", "W2"}, set(), cy, wm, ws, 2027), [row(1990), row(1991)]),
 ("one year two works + a series", W({1990}, {"W1", "W2", "W3"}, set(), cy, wm, ws, 2027), [row(1990, None, 500, 77)]),
 ("no P585, ceremony with year", W(set(), set(), {"C82"}, cy, wm, ws, 2027), [row(1982, "C82")]),
 ("no P585, ceremony without year", W(set(), set(), {"Cx"}, cy, wm, ws, 2027), [row(None, "Cx")]),
 ("no P585, two ceremonies two years", W(set(), set(), {"C82", "C83"}, cy, wm, ws, 2027), [row(1982, "C82"), row(1983, "C83")]),
 ("no P585, two ceremonies same year", W(set(), set(), {"C82", "C82b"}, cy, wm, ws, 2027), [row(1982, None)]),
 ("year out of range only", W({15}, {"W1"}, set(), cy, wm, ws, 2027), [row(None, None, 11816)]),
 ("year out of range + valid", W({19577, 1977}, set(), set(), cy, wm, ws, 2027), [row(1977)]),
 ("year too far ahead", W({2028}, set(), set(), cy, wm, ws, 2027), [row(None)]),
 ("year + ceremony of that year", W({1982}, set(), {"C82", "C83"}, cy, wm, ws, 2027), [row(1982, "C82")]),
 ("year + single ceremony unknown year", W({1982}, set(), {"Cx"}, cy, wm, ws, 2027), [row(1982, "Cx")]),
 ("two years, one ceremony", W({1982, 1983}, set(), {"C83"}, cy, wm, ws, 2027), [row(1982), row(1983, "C83")]),
 ("work not in T2S", W({1982}, {"W9"}, set(), cy, wm, ws, 2027), [row(1982)]),
]
bad = 0
for name, got, want in cases:
    ok = got == want
    bad += not ok
    print("PASS" if ok else "FAIL", name, "" if ok else f"\n   got  {got}\n   want {want}")

lines = [
 {"ID_PERSON": 6598, "POPULARITY": 9, "ID_STATEMENT": 10, "ID_QUALIFIER_PROPERTY": "P585", "YEAR_VALUE": 1934, "ID_QUALIFIER_ITEM": None},
 {"ID_PERSON": 6598, "POPULARITY": 9, "ID_STATEMENT": 10, "ID_QUALIFIER_PROPERTY": "P1686", "YEAR_VALUE": None, "ID_QUALIFIER_ITEM": "W1"},
 {"ID_PERSON": 6598, "POPULARITY": 9, "ID_STATEMENT": 11, "ID_QUALIFIER_PROPERTY": "P805", "YEAR_VALUE": None, "ID_QUALIFIER_ITEM": "C82"},
 {"ID_PERSON": 7, "POPULARITY": 3, "ID_STATEMENT": 12, "ID_QUALIFIER_PROPERTY": None, "YEAR_VALUE": None, "ID_QUALIFIER_ITEM": None},
]
e = h.f_awardwinentities(lines, "ID_PERSON")
want = [{"id": 6598, "statements": [{"id": 10, "years": {1934}, "works": {"W1"}, "ceremonies": set()}, {"id": 11, "years": set(), "works": set(), "ceremonies": {"C82"}}]},
        {"id": 7, "statements": [{"id": 12, "years": set(), "works": set(), "ceremonies": set()}]}]
ok = e == want; bad += not ok; print("PASS" if ok else "FAIL", "f_awardwinentities", "" if ok else e)

cand = [
 {"ID_AWARD": 1, "ID_AWARD_FAMILY": 100, "ID_FAMILY_WIKIDATA": "Q19020", "ROUTE": "P361"},
 {"ID_AWARD": 2, "ID_AWARD_FAMILY": 100, "ID_FAMILY_WIKIDATA": "Q19020", "ROUTE": "P31"},
 {"ID_AWARD": 2, "ID_AWARD_FAMILY": 200, "ID_FAMILY_WIKIDATA": "Q5", "ROUTE": "P31"},      # smaller family loses
 {"ID_AWARD": 3, "ID_AWARD_FAMILY": 300, "ID_FAMILY_WIKIDATA": "Q900", "ROUTE": "P527"},
 {"ID_AWARD": 3, "ID_AWARD_FAMILY": 400, "ID_FAMILY_WIKIDATA": "Q800", "ROUTE": "P361"},   # same size, stronger route
 {"ID_AWARD": 4, "ID_AWARD_FAMILY": 500, "ID_FAMILY_WIKIDATA": "Q70", "ROUTE": "P31"},
 {"ID_AWARD": 4, "ID_AWARD_FAMILY": 600, "ID_FAMILY_WIKIDATA": "Q60", "ROUTE": "P31"},     # same size and route, lowest Q
]
got = h.f_awardfamilypick(cand); want = {1: 100, 2: 100, 3: 400, 4: 600}
ok = got == want; bad += not ok; print("PASS" if ok else "FAIL", "f_awardfamilypick", "" if ok else got)
ok = h.f_awardfamilypick([]) == {}; bad += not ok; print("PASS" if ok else "FAIL", "f_awardfamilypick empty")
print("RESULT:", "PASS" if not bad else f"{bad} FAILED")
sys.exit(1 if bad else 0)

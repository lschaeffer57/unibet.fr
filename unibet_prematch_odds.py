#!/usr/bin/env python3
"""Toutes les cotes **prématch** Unibet via l’API ``lvs-api`` (HTTP, sans Playwright).

Enchaîne les listings ``/lvs-api/next/50/…`` (par sport puis par ligue) puis un détail
``/lvs-api/ff/e{id}`` par match pour récupérer **tous** les marchés retournés par le site.

Le jeton ``X-LVS-HSToken`` est extrait de la page sport (``serverApp-state``), comme le navigateur.

Exemples ::
    python unibet_prematch_odds.py -o output.json
    python unibet_prematch_odds.py --limit-events 20 -o sample.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent

try:
    import orjson as _orjson
except ImportError:
    _orjson = None

from unibet_capture import build_output_sports_document
from unibet_prematch_core import SPORT_NODES, build_output
from unibet_lvs_http import (
    fetch_all_prematch_event_meta,
    fetch_lvs_hs_token,
    fetch_prematch_event_details,
    lvs_http_session,
)


def prematch_nested_to_rows(doc: dict) -> list[dict]:
    """Aplatit la sortie ``build_output`` en lignes pour ``build_output_sports_document``."""
    rows: list[dict] = []
    for sport, comps in (doc.get("sports") or {}).items():
        for comp_name, events in comps.items():
            for ev in events:
                match = (ev.get("name") or "").strip() or "?"
                for m in ev.get("markets") or []:
                    period = (m.get("period_desc") or "FT").strip() or "FT"
                    mname = (m.get("desc") or "").strip()
                    for o in m.get("outcomes") or []:
                        pr = o.get("price")
                        if pr is None:
                            continue
                        rows.append(
                            {
                                "sport": sport,
                                "competition": comp_name,
                                "match": match,
                                "market": mname,
                                "period": period,
                                "selection": (o.get("desc") or "").strip(),
                                "odds": float(pr),
                            }
                        )
    return rows


def parse_sport_filter(arg: str | None) -> dict[str, str] | None:
    if not arg or not str(arg).strip():
        return None
    keys = [x.strip().lower() for x in str(arg).split(",") if x.strip()]
    out: dict[str, str] = {}
    for k in keys:
        if k not in SPORT_NODES:
            print(f"[unibet_prematch_odds] sport inconnu ignoré : {k!r} (connus : {list(SPORT_NODES)})", file=sys.stderr)
            continue
        out[k] = SPORT_NODES[k]
    return out or None


def write_json(path: Path, payload: dict, *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if _orjson is not None:
        opts = _orjson.OPT_INDENT_2 if pretty else 0
        raw = _orjson.dumps(payload, option=opts)
        if pretty and not raw.endswith(b"\n"):
            raw += b"\n"
        path.write_bytes(raw)
    else:
        with open(path, "w", encoding="utf-8") as f:
            if pretty:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            else:
                json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            f.write("\n")


async def run_async(
    *,
    limit_events: int | None,
    sports_filter: dict[str, str] | None,
    detail_concurrency: int,
) -> tuple[dict, dict]:
    session = await lvs_http_session()
    try:
        token = await fetch_lvs_hs_token(session)
        events_meta = await fetch_all_prematch_event_meta(
            session, token, sport_nodes=sports_filter
        )
        listed = len(events_meta)
        eids = list(events_meta.keys())
        if limit_events is not None and limit_events > 0:
            eids = eids[:limit_events]
            events_meta = {k: events_meta[k] for k in eids}
        events_detail = await fetch_prematch_event_details(
            session, token, eids, concurrency=detail_concurrency
        )
        ok = sum(1 for d in events_detail.values() if not d.get("_error"))
        err = len(events_detail) - ok
        nested = build_output(events_meta, events_detail)
        rows = prematch_nested_to_rows(nested)
        doc = build_output_sports_document(rows)
        meta = {
            "schema": "unibet_odds_v1",
            "source": "unibet_prematch_odds",
            "feed": "lvs-api",
            "prematch": True,
            "events_listed": listed,
            "events_fetched": len(eids),
            "detail_ok": ok,
            "detail_errors": err,
            "total_selection_rows": len(rows),
            "sports_nodes": list((sports_filter or SPORT_NODES).keys()),
        }
        doc["meta"] = {
            **meta,
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        return doc, meta
    finally:
        await session.close()


def main() -> None:
    default_out = BASE / "output_prematch.json"
    p = argparse.ArgumentParser(description="Cotes prématch Unibet (lvs-api, tous les marchés par match)")
    p.add_argument("-o", "--output", type=Path, default=default_out)
    p.add_argument(
        "--limit-events",
        type=int,
        default=0,
        help="Ne traiter que les N premiers matchs (0 = tous)",
    )
    p.add_argument(
        "--sports",
        type=str,
        default=os.environ.get("UNIBET_PREMATCH_SPORTS", "").strip() or None,
        help="Liste football,tennis,basketball,hockey (défaut : les 4)",
    )
    p.add_argument(
        "--detail-concurrency",
        type=int,
        default=int(os.environ.get("UNIBET_PREMATCH_CONCURRENCY", "40")),
        help="Requêtes ff/e en parallèle (défaut 40)",
    )
    p.add_argument("--compact", action="store_true", help="JSON sur une ligne")
    args = p.parse_args()

    lim = args.limit_events if args.limit_events and args.limit_events > 0 else None
    sports = parse_sport_filter(args.sports)

    doc, meta = asyncio.run(
        run_async(
            limit_events=lim,
            sports_filter=sports,
            detail_concurrency=max(1, args.detail_concurrency),
        )
    )
    write_json(args.output, doc, pretty=not args.compact)
    print(
        f"Écrit : {args.output} — {meta.get('total_selection_rows', '?')} lignes cotes, "
        f"{meta.get('detail_ok', '?')}/{meta.get('events_fetched', '?')} détails OK",
        flush=True,
    )


if __name__ == "__main__":
    main()

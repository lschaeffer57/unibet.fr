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
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent

LOG = logging.getLogger("unibet.prematch")


class _UtcFormatter(logging.Formatter):
    """Horodatage UTC (lisible dans les logs Railway)."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{int(record.msecs):03d}Z"


def setup_logging() -> None:
    level_name = (os.environ.get("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(level)
    h.setFormatter(
        _UtcFormatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    root.addHandler(h)


def _log_kv(cycle: int | None, msg: str, **fields: Any) -> None:
    parts = [msg]
    if cycle is not None:
        parts.insert(0, f"cycle={cycle}")
    for k, v in fields.items():
        parts.append(f"{k}={v}")
    LOG.info(" | ".join(parts))

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
    cycle: int | None = None,
) -> tuple[dict, dict]:
    t_run0 = time.perf_counter()
    t_sess0 = time.perf_counter()
    session = await lvs_http_session()
    ms_sess = int((time.perf_counter() - t_sess0) * 1000)
    _log_kv(cycle, "phase=session", duration_ms=ms_sess)
    try:
        t0 = time.perf_counter()
        token = await fetch_lvs_hs_token(session)
        ms_token = int((time.perf_counter() - t0) * 1000)
        _log_kv(cycle, "phase=token", duration_ms=ms_token, status="ok")

        t1 = time.perf_counter()
        events_meta = await fetch_all_prematch_event_meta(
            session, token, sport_nodes=sports_filter
        )
        listed = len(events_meta)
        ms_list = int((time.perf_counter() - t1) * 1000)
        _log_kv(
            cycle,
            "phase=listing",
            duration_ms=ms_list,
            events_listed=listed,
        )

        eids = list(events_meta.keys())
        if limit_events is not None and limit_events > 0:
            eids = eids[:limit_events]
            events_meta = {k: events_meta[k] for k in eids}
        n_fetch = len(eids)

        t2 = time.perf_counter()
        events_detail = await fetch_prematch_event_details(
            session, token, eids, concurrency=detail_concurrency
        )
        ms_detail = int((time.perf_counter() - t2) * 1000)
        ok = sum(1 for d in events_detail.values() if not d.get("_error"))
        err = len(events_detail) - ok
        _log_kv(
            cycle,
            "phase=details",
            duration_ms=ms_detail,
            events_fetched=n_fetch,
            detail_ok=ok,
            detail_errors=err,
            concurrency=detail_concurrency,
        )

        t3 = time.perf_counter()
        nested = build_output(events_meta, events_detail)
        rows = prematch_nested_to_rows(nested)
        doc = build_output_sports_document(rows)
        ms_build = int((time.perf_counter() - t3) * 1000)
        _log_kv(
            cycle,
            "phase=build_json",
            duration_ms=ms_build,
            selection_rows=len(rows),
        )

        total_ms = int((time.perf_counter() - t_run0) * 1000)
        timings = {
            "session_ms": ms_sess,
            "token_ms": ms_token,
            "listing_ms": ms_list,
            "details_ms": ms_detail,
            "build_ms": ms_build,
            "total_ms": total_ms,
        }
        meta = {
            "schema": "unibet_odds_v1",
            "source": "unibet_prematch_odds",
            "feed": "lvs-api",
            "prematch": True,
            "events_listed": listed,
            "events_fetched": n_fetch,
            "detail_ok": ok,
            "detail_errors": err,
            "total_selection_rows": len(rows),
            "sports_nodes": list((sports_filter or SPORT_NODES).keys()),
            "timings": timings,
        }
        doc["meta"] = {
            **meta,
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        _log_kv(
            cycle,
            "phase=summary",
            status="ok",
            total_ms=total_ms,
            events_listed=listed,
            events_fetched=n_fetch,
            detail_ok=ok,
            detail_errors=err,
            selection_rows=len(rows),
        )
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
    p.add_argument(
        "--loop-seconds",
        type=int,
        default=int(os.environ.get("SCRAPER_LOOP_SECONDS") or "0"),
        help="Répéter le run toutes les N secondes (0 = une fois). Env : SCRAPER_LOOP_SECONDS",
    )
    args = p.parse_args()

    setup_logging()
    lim = args.limit_events if args.limit_events and args.limit_events > 0 else None
    sports = parse_sport_filter(args.sports)
    loop_s = max(0, args.loop_seconds)

    cycle = 0
    while True:
        cycle += 1
        t_cycle0 = time.perf_counter()
        _log_kv(
            cycle,
            "event=cycle_start",
            loop_seconds=loop_s,
            output=str(args.output),
        )
        try:
            doc, meta = asyncio.run(
                run_async(
                    limit_events=lim,
                    sports_filter=sports,
                    detail_concurrency=max(1, args.detail_concurrency),
                    cycle=cycle,
                )
            )
            write_json(args.output, doc, pretty=not args.compact)
        except Exception:
            LOG.exception(" | ".join([f"cycle={cycle}", "event=cycle_failed"]))
            if loop_s <= 0:
                raise
            _log_kv(
                cycle,
                "event=sleep_after_error",
                sleep_s=min(loop_s, 120),
            )
            time.sleep(min(loop_s, 120))
            continue

        wall_ms = int((time.perf_counter() - t_cycle0) * 1000)
        _log_kv(
            cycle,
            "event=cycle_end",
            status="ok",
            wall_ms=wall_ms,
            file=str(args.output),
            rows=meta.get("total_selection_rows"),
            detail_ok=meta.get("detail_ok"),
            events_fetched=meta.get("events_fetched"),
        )
        print(
            f"Écrit : {args.output} — {meta.get('total_selection_rows', '?')} lignes cotes, "
            f"{meta.get('detail_ok', '?')}/{meta.get('events_fetched', '?')} détails OK",
            flush=True,
        )

        if loop_s <= 0:
            break
        _log_kv(cycle, "event=sleep_until_next", sleep_s=loop_s)
        time.sleep(loop_s)


if __name__ == "__main__":
    main()

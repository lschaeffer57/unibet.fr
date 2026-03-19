"""Export Unibet → JSON (schéma output). `python unibet_all_json.py` | `-o f.json` | `--legacy-tables`."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from unibet_normalize_output import build_output_json_document

try:
    import orjson as _orjson
except ImportError:
    _orjson = None


def _stdlib_json_default(obj):
    """Repli si orjson échoue : numpy / types exotiques dans le payload."""
    if hasattr(obj, "item") and callable(getattr(obj, "item")):
        try:
            return obj.item()
        except Exception:
            pass
    raise TypeError(f"Non sérialisable JSON: {type(obj).__name__}")


def _df_to_records(df: pd.DataFrame) -> list:
    if df is None or df.empty:
        return []
    raw = df.to_json(orient="records", date_format="iso")
    if not raw:
        return []
    return json.loads(raw)


def _pack_sport_legacy(mod, sport_key: str) -> dict:
    tables = {}
    counts = {}
    for key, frame in mod.get_export_tables():
        tables[key] = _df_to_records(frame)
        counts[key] = len(frame)
    return {
        "sport": sport_key,
        "python_module": getattr(mod, "__name__", ""),
        "unibet_sport_node_id": getattr(mod, "UNIBET_SPORT_NODE_ID", None),
        "row_counts": counts,
        "markets": tables,
    }


async def run_scrapes_only():
    import unibet_t_async as tennis_mod
    import unibet_f_async as foot_mod
    import unibet_b_async as basket_mod
    import unibet_h_async as hockey_mod

    # Les 4 sports sont indépendants : paralléliser ~divise le temps réel par rapport à l’enchaînement séquentiel.
    await asyncio.gather(
        tennis_mod.run_scrape(),
        foot_mod.run_scrape(),
        basket_mod.run_scrape(),
        hockey_mod.run_scrape(),
    )
    return tennis_mod, foot_mod, basket_mod, hockey_mod


async def run_all_scrapes_legacy_json() -> dict:
    tennis_mod, foot_mod, basket_mod, hockey_mod = await run_scrapes_only()
    generated = datetime.now(timezone.utc).isoformat()
    return {
        "meta": {
            "generated_at_utc": generated,
            "bookmaker": "unibet.fr",
            "scraping_spec": "scraping_specifications_clean.pdf",
        },
        "code_link": {
            "unibet_t_async.py": {"sport": "tennis", "unibet_sport_node_id": tennis_mod.UNIBET_SPORT_NODE_ID},
            "unibet_f_async.py": {"sport": "football", "unibet_sport_node_id": foot_mod.UNIBET_SPORT_NODE_ID},
            "unibet_b_async.py": {"sport": "basketball", "unibet_sport_node_id": basket_mod.UNIBET_SPORT_NODE_ID},
            "unibet_h_async.py": {"sport": "hockey", "unibet_sport_node_id": hockey_mod.UNIBET_SPORT_NODE_ID},
        },
        "data": {
            "tennis": _pack_sport_legacy(tennis_mod, "tennis"),
            "football": _pack_sport_legacy(foot_mod, "football"),
            "basketball": _pack_sport_legacy(basket_mod, "basketball"),
            "hockey": _pack_sport_legacy(hockey_mod, "hockey"),
        },
    }


async def run_output_format_json() -> dict:
    tennis_mod, foot_mod, basket_mod, hockey_mod = await run_scrapes_only()
    return build_output_json_document(tennis_mod, foot_mod, basket_mod, hockey_mod)


def main():
    parser = argparse.ArgumentParser(description="Export JSON Unibet (schéma output.json par défaut)")
    default_out = Path(__file__).resolve().parent / "output.json"
    parser.add_argument("-o", "--output", type=Path, default=default_out, help="Fichier JSON de sortie")
    parser.add_argument(
        "--legacy-tables",
        action="store_true",
        help="Ancien format avec data.tennis.markets DataFrames sérialisés",
    )
    parser.add_argument("--pretty", action="store_true", help="JSON indenté (défaut : une ligne compacte)")
    args = parser.parse_args()

    if args.legacy_tables:
        payload = asyncio.run(run_all_scrapes_legacy_json())
    else:
        payload = asyncio.run(run_output_format_json())

    args.output.parent.mkdir(parents=True, exist_ok=True)

    def _write_json_stdlib() -> None:
        with open(args.output, "w", encoding="utf-8") as f:
            if args.pretty:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=_stdlib_json_default)
            else:
                json.dump(
                    payload, f, ensure_ascii=False, separators=(",", ":"), default=_stdlib_json_default
                )

    if _orjson is not None:
        opts = _orjson.OPT_INDENT_2 if args.pretty else 0
        numpy_opt = getattr(_orjson, "OPT_SERIALIZE_NUMPY", 0)
        try:
            raw = _orjson.dumps(payload, option=opts | numpy_opt)
        except TypeError:
            _write_json_stdlib()
        else:
            suffix = b"\n" if args.pretty and not raw.endswith(b"\n") else b""
            args.output.write_bytes(raw + suffix)
    else:
        _write_json_stdlib()

    print(f"Écrit : {args.output} ({args.output.stat().st_size // 1024} Ko env.)")


if __name__ == "__main__":
    main()

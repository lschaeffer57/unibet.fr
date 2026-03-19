"""DataFrames scrapers → document JSON (sports → competitions → match/date/url/markets/props)."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import pytz

PARIS = pytz.timezone("Europe/Paris")


def normalize_match_url(lien: str | None) -> str:
    """Colonne scraper « lien » → URL https absolue ouvrant la fiche match (/sport/.../event/...html)."""
    if not lien or not isinstance(lien, str):
        return ""
    s = lien.strip()
    if not s:
        return ""
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return s
    if low.startswith("www.unibet.fr"):
        return "https://" + s
    # Chemin canonique fiche événement
    pos = low.find("/sport/")
    if pos != -1:
        path = s[pos:]
        return "https://www.unibet.fr" + (path if path.startswith("/") else "/" + path)
    if low.startswith("unibet.fr"):
        rest = s[9:].lstrip("/")
        return "https://www.unibet.fr/" + rest
    return "https://www.unibet.fr/" + s.lstrip("/")


def teams_from_match(nom: str) -> tuple[str, str]:
    if not nom or not isinstance(nom, str):
        return "", ""
    if " - " in nom:
        a, b = nom.split(" - ", 1)
        return a.strip(), b.strip()
    return nom.strip(), ""


def fr_datetime_to_iso(date_str: str) -> str:
    if not date_str or not isinstance(date_str, str):
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y %H:%M:%S")
        aware = PARIS.localize(dt)
        return aware.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return ""


def generated_at_output_format() -> str:
    return datetime.now(PARIS).strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(x) -> float | None:
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except TypeError:
        pass
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return round(v, 4)
    except (ValueError, TypeError):
        return None


def _m(market: str, period: str, selection: str, odds) -> dict | None:
    o = _safe_float(odds)
    if o is None:
        return None
    return {
        "market": market,
        "period": period,
        "selection": str(selection).strip(),
        "odds": round(o, 2),
    }


def _extract_line_from_cut(cut: str | None) -> str | None:
    if not cut or not isinstance(cut, str):
        return None
    s = cut.replace(",", ".")
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return None
    return nums[-1]


def _tennis_table_to_markets(table_key: str, row: pd.Series, home: str, away: str) -> list[dict]:
    r = row
    out: list[dict | None] = []

    if table_key == "Win":
        out.extend([_m("moneyline", "FT", home, r.get("cote 1")), _m("moneyline", "FT", away, r.get("cote 2"))])
    elif table_key == "Winset1":
        out.extend([_m("moneyline", "S1", home, r.get("cote 1")), _m("moneyline", "S1", away, r.get("cote 2"))])
    elif table_key == "OUJEU":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([_m("total", "FT", f"Over {ln}", r.get("over")), _m("total", "FT", f"Under {ln}", r.get("under"))])
    elif table_key == "OUJEUSet1":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([_m("total", "S1", f"Over {ln}", r.get("over")), _m("total", "S1", f"Under {ln}", r.get("under"))])
    elif table_key == "OUJoueur1":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "FT", f"{home} Over {ln}", r.get("over")),
                _m("total", "FT", f"{home} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUJoueur2":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "FT", f"{away} Over {ln}", r.get("over")),
                _m("total", "FT", f"{away} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUJoueur1Set1":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "S1", f"{home} Over {ln}", r.get("over")),
                _m("total", "S1", f"{home} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUJoueur2Set1":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "S1", f"{away} Over {ln}", r.get("over")),
                _m("total", "S1", f"{away} Under {ln}", r.get("under")),
            ])
    elif table_key == "HDPSet":
        c1, c2 = r.get("cut 1"), r.get("cut 2")
        if c1 is not None and str(c1).strip():
            out.append(_m("spread", "FT", f"{home} {c1}", r.get("cote 1")))
        if c2 is not None and str(c2).strip():
            out.append(_m("spread", "FT", f"{away} {c2}", r.get("cote 2")))
    elif table_key == "HDJeuxFT":
        c1, c2 = r.get("cut 1"), r.get("cut 2")
        if c1 is not None and str(c1).strip():
            out.append(_m("spread", "FT", f"{home} {c1}", r.get("cote 1")))
        if c2 is not None and str(c2).strip():
            out.append(_m("spread", "FT", f"{away} {c2}", r.get("cote 2")))
    elif table_key == "HDJeuxSet1":
        c1, c2 = r.get("cut 1"), r.get("cut 2")
        if c1 is not None and str(c1).strip():
            out.append(_m("spread", "S1", f"{home} {c1}", r.get("cote 1")))
        if c2 is not None and str(c2).strip():
            out.append(_m("spread", "S1", f"{away} {c2}", r.get("cote 2")))

    return [x for x in out if x is not None]


def _foot_table_to_markets(table_key: str, row: pd.Series, home: str, away: str) -> list[dict]:
    r = row
    out: list[dict | None] = []

    if table_key == "Win":
        out.extend([
            _m("moneyline", "FT", home, r.get("cote 1")),
            _m("moneyline", "FT", "Draw", r.get("cote N")),
            _m("moneyline", "FT", away, r.get("cote 2")),
        ])
    elif table_key == "WinHT":
        out.extend([
            _m("moneyline", "HT", home, r.get("cote 1")),
            _m("moneyline", "HT", "Draw", r.get("cote N")),
            _m("moneyline", "HT", away, r.get("cote 2")),
        ])
    elif table_key == "C2":
        out.extend([
            _m("double_chance", "FT", "1X", r.get("1N")),
            _m("double_chance", "FT", "12", r.get("12")),
            _m("double_chance", "FT", "X2", r.get("N2")),
        ])
    elif table_key == "C2HT":
        out.extend([
            _m("double_chance", "HT", "1X", r.get("1N")),
            _m("double_chance", "HT", "12", r.get("12")),
            _m("double_chance", "HT", "X2", r.get("N2")),
        ])
    elif table_key == "WinHT_DNB":
        out.extend([
            _m("moneyline", "HT", home, r.get("cote 1")),
            _m("moneyline", "HT", away, r.get("cote 2")),
        ])
    elif table_key == "DNB_FT":
        out.extend([
            _m("moneyline", "FT", home, r.get("cote 1")),
            _m("moneyline", "FT", away, r.get("cote 2")),
        ])
    elif table_key == "OUMatch":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([_m("total", "FT", f"Over {ln}", r.get("over")), _m("total", "FT", f"Under {ln}", r.get("under"))])
    elif table_key == "OUHT":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([_m("total", "HT", f"Over {ln}", r.get("over")), _m("total", "HT", f"Under {ln}", r.get("under"))])
    elif table_key == "OU2H":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([_m("total", "H2", f"Over {ln}", r.get("over")), _m("total", "H2", f"Under {ln}", r.get("under"))])
    elif table_key == "OUTeamHome_FT":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "FT", f"{home} Over {ln}", r.get("over")),
                _m("total", "FT", f"{home} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUTeamAway_FT":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "FT", f"{away} Over {ln}", r.get("over")),
                _m("total", "FT", f"{away} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUTeamHome_HT":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "HT", f"{home} Over {ln}", r.get("over")),
                _m("total", "HT", f"{home} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUTeamAway_HT":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "HT", f"{away} Over {ln}", r.get("over")),
                _m("total", "HT", f"{away} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUTeamHome_2H":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "H2", f"{home} Over {ln}", r.get("over")),
                _m("total", "H2", f"{home} Under {ln}", r.get("under")),
            ])
    elif table_key == "OUTeamAway_2H":
        ln = _extract_line_from_cut(r.get("cut"))
        if ln:
            out.extend([
                _m("total", "H2", f"{away} Over {ln}", r.get("over")),
                _m("total", "H2", f"{away} Under {ln}", r.get("under")),
            ])
    elif table_key == "BTTS_FT":
        out.extend([_m("btts", "FT", "Oui", r.get("oui")), _m("btts", "FT", "Non", r.get("non"))])
    elif table_key == "BTTS_HT":
        out.extend([_m("btts", "HT", "Oui", r.get("oui")), _m("btts", "HT", "Non", r.get("non"))])
    elif table_key == "BTTS_2H":
        out.extend([_m("btts", "H2", "Oui", r.get("oui")), _m("btts", "H2", "Non", r.get("non"))])
    elif table_key == "FirstGoal_FT":
        out.extend([
            _m("first_goal", "FT", home, r.get("cote 1")),
            _m("first_goal", "FT", "Aucun but", r.get("cote N")),
            _m("first_goal", "FT", away, r.get("cote 2")),
        ])
    elif table_key == "FirstGoal_HT":
        out.extend([
            _m("first_goal", "HT", home, r.get("cote 1")),
            _m("first_goal", "HT", "Aucun but", r.get("cote N")),
            _m("first_goal", "HT", away, r.get("cote 2")),
        ])
    elif table_key == "WinToNil_Home_FT":
        out.extend([
            _m("yes_no", "FT", f"{home} win to nil — Oui", r.get("oui")),
            _m("yes_no", "FT", f"{home} win to nil — Non", r.get("non")),
        ])
    elif table_key == "WinToNil_Away_FT":
        out.extend([
            _m("yes_no", "FT", f"{away} win to nil — Oui", r.get("oui")),
            _m("yes_no", "FT", f"{away} win to nil — Non", r.get("non")),
        ])
    elif table_key == "HDP_FT_3":
        cut = r.get("cut") or ""
        base = str(cut)[:120]
        out.extend([
            _m("spread", "FT", f"{base} — opt.1", r.get("cote 1")),
            _m("spread", "FT", f"{base} — opt.N", r.get("cote N")),
            _m("spread", "FT", f"{base} — opt.2", r.get("cote 2")),
        ])
    elif table_key == "HDP_HT_3":
        cut = r.get("cut") or ""
        base = str(cut)[:120]
        out.extend([
            _m("spread", "HT", f"{base} — opt.1", r.get("cote 1")),
            _m("spread", "HT", f"{base} — opt.N", r.get("cote N")),
            _m("spread", "HT", f"{base} — opt.2", r.get("cote 2")),
        ])
    elif table_key == "HDP_2H_3":
        cut = r.get("cut") or ""
        base = str(cut)[:120]
        out.extend([
            _m("spread", "H2", f"{base} — opt.1", r.get("cote 1")),
            _m("spread", "H2", f"{base} — opt.N", r.get("cote N")),
            _m("spread", "H2", f"{base} — opt.2", r.get("cote 2")),
        ])

    return [x for x in out if x is not None]


def _bb_period_for_total_key(table_key: str) -> str | None:
    return {
        "OUMatch": "FT",
        "OUHT": "HT",
        "OUQ1": "Q1",
        "OUQ2": "Q2",
        "OUMatch1": "FT",
        "OUMatch2": "FT",
        "OUQ1_Home": "Q1",
        "OUQ1_Away": "Q1",
        "OUQ2_Home": "Q2",
        "OUQ2_Away": "Q2",
    }.get(table_key)


def _bb_spread_period(table_key: str) -> str | None:
    return {
        "HDPPoints": "FT",
        "HDPHT": "HT",
        "HDPQ1": "Q1",
        "HDPQ2": "Q2",
    }.get(table_key)


def _basket_prop_rows_from_df_row(table_key: str, row: pd.Series) -> list[dict]:
    if table_key != "PropsNBA":
        return []
    o = _safe_float(row.get("odds"))
    if o is None:
        return []
    return [{
        "player": str(row.get("player") or ""),
        "stat": str(row.get("stat") or ""),
        "selection": str(row.get("selection") or ""),
        "odds": round(o, 2),
    }]


def _hockey_prop_rows_from_df_row(table_key: str, row: pd.Series) -> list[dict]:
    if table_key != "PropsHockey":
        return []
    o = _safe_float(row.get("odds"))
    if o is None:
        return []
    return [{
        "player": str(row.get("player") or ""),
        "stat": str(row.get("stat") or ""),
        "selection": str(row.get("selection") or ""),
        "odds": round(o, 2),
    }]


def _hk_ou_period(table_key: str) -> str | None:
    return {
        "OURT": "RT",
        "OUET": "ET",
        "OUHome_RT": "RT",
        "OUAway_RT": "RT",
        "OUP1": "P1",
        "OUP2": "P2",
        "OUP3": "P3",
        "OUP1_Home": "P1",
        "OUP1_Away": "P1",
        "OUP2_Home": "P2",
        "OUP2_Away": "P2",
        "OUP3_Home": "P3",
        "OUP3_Away": "P3",
    }.get(table_key)


def _hk_spread_period(table_key: str) -> str | None:
    return {
        "HDP_OTPI": "ET",
        "HDP_OTSO": "ET",
        "HDP_P1": "P1",
        "HDP_P2": "P2",
        "HDP_P3": "P3",
    }.get(table_key)


def _hk_dc_period(table_key: str) -> str | None:
    return {
        "C2_RT": "RT",
        "C2_P1": "P1",
        "C2_P2": "P2",
        "C2_P3": "P3",
    }.get(table_key)


def _hk_btts_period(table_key: str) -> str | None:
    return {
        "BTTS_RT": "RT",
        "BTTS_P1": "P1",
        "BTTS_P2": "P2",
        "BTTS_P3": "P3",
    }.get(table_key)


def _hk_first_goal_period(table_key: str) -> str | None:
    return {
        "FirstGoal_RT": "RT",
        "FirstGoal_P1": "P1",
        "FirstGoal_P2": "P2",
        "FirstGoal_P3": "P3",
    }.get(table_key)


def _hockey_table_to_markets(table_key: str, row: pd.Series, home: str, away: str) -> list[dict]:
    if table_key == "PropsHockey":
        return []

    r = row
    out: list[dict | None] = []

    if table_key == "WinRT":
        out.extend([
            _m("moneyline", "RT", home, r.get("cote 1")),
            _m("moneyline", "RT", "Draw", r.get("cote N")),
            _m("moneyline", "RT", away, r.get("cote 2")),
        ])
    elif table_key == "WinP1":
        out.extend([
            _m("moneyline", "P1", home, r.get("cote 1")),
            _m("moneyline", "P1", "Draw", r.get("cote N")),
            _m("moneyline", "P1", away, r.get("cote 2")),
        ])
    elif table_key == "WinP2":
        out.extend([
            _m("moneyline", "P2", home, r.get("cote 1")),
            _m("moneyline", "P2", "Draw", r.get("cote N")),
            _m("moneyline", "P2", away, r.get("cote 2")),
        ])
    elif table_key == "WinP3":
        out.extend([
            _m("moneyline", "P3", home, r.get("cote 1")),
            _m("moneyline", "P3", "Draw", r.get("cote N")),
            _m("moneyline", "P3", away, r.get("cote 2")),
        ])
    elif table_key == "WinET":
        out.extend([_m("moneyline", "ET", home, r.get("cote 1")), _m("moneyline", "ET", away, r.get("cote 2"))])
    elif table_key == "DNB_RT":
        out.extend([_m("moneyline", "RT", home, r.get("cote 1")), _m("moneyline", "RT", away, r.get("cote 2"))])
    elif table_key == "DNB_P1":
        out.extend([_m("moneyline", "P1", home, r.get("cote 1")), _m("moneyline", "P1", away, r.get("cote 2"))])
    elif table_key == "DNB_P2":
        out.extend([_m("moneyline", "P2", home, r.get("cote 1")), _m("moneyline", "P2", away, r.get("cote 2"))])
    elif table_key == "DNB_P3":
        out.extend([_m("moneyline", "P3", home, r.get("cote 1")), _m("moneyline", "P3", away, r.get("cote 2"))])
    else:
        per_ou = _hk_ou_period(table_key)
        if per_ou:
            ln = _extract_line_from_cut(r.get("cut"))
            if ln:
                label = ""
                if table_key == "OUHome_RT":
                    label = f"{home} "
                elif table_key == "OUAway_RT":
                    label = f"{away} "
                elif table_key == "OUP1_Home":
                    label = f"{home} "
                elif table_key == "OUP1_Away":
                    label = f"{away} "
                elif table_key == "OUP2_Home":
                    label = f"{home} "
                elif table_key == "OUP2_Away":
                    label = f"{away} "
                elif table_key == "OUP3_Home":
                    label = f"{home} "
                elif table_key == "OUP3_Away":
                    label = f"{away} "
                out.extend([
                    _m("total", per_ou, f"{label}Over {ln}", r.get("over")),
                    _m("total", per_ou, f"{label}Under {ln}", r.get("under")),
                ])
        spr = _hk_spread_period(table_key)
        if spr:
            c1, c2 = r.get("cut 1"), r.get("cut 2")
            if c1 is not None and str(c1).strip():
                out.append(_m("spread", spr, f"{home} {c1}", r.get("cote 1")))
            if c2 is not None and str(c2).strip():
                out.append(_m("spread", spr, f"{away} {c2}", r.get("cote 2")))
        dc = _hk_dc_period(table_key)
        if dc:
            out.extend([
                _m("double_chance", dc, "1X", r.get("1N")),
                _m("double_chance", dc, "12", r.get("12")),
                _m("double_chance", dc, "X2", r.get("N2")),
            ])
        bt = _hk_btts_period(table_key)
        if bt:
            out.extend([_m("btts", bt, "Oui", r.get("oui")), _m("btts", bt, "Non", r.get("non"))])
        fg = _hk_first_goal_period(table_key)
        if fg:
            out.extend([
                _m("first_goal", fg, home, r.get("cote 1")),
                _m("first_goal", fg, "Aucun but", r.get("cote N")),
                _m("first_goal", fg, away, r.get("cote 2")),
            ])
        if table_key == "OT_YESNO":
            out.extend([_m("yes_no", "RT", "Oui", r.get("oui")), _m("yes_no", "RT", "Non", r.get("non"))])

    return [x for x in out if x is not None]


def _basket_table_to_markets(table_key: str, row: pd.Series, home: str, away: str) -> list[dict]:
    if table_key == "PropsNBA":
        return []

    r = row
    out: list[dict | None] = []

    if table_key == "WinHP":
        out.extend([
            _m("moneyline", "FT", home, r.get("cote 1")),
            _m("moneyline", "FT", "Draw", r.get("cote N")),
            _m("moneyline", "FT", away, r.get("cote 2")),
        ])
    elif table_key == "Win":
        out.extend([_m("moneyline", "FT", home, r.get("cote 1")), _m("moneyline", "FT", away, r.get("cote 2"))])
    elif table_key == "WinHT":
        out.extend([
            _m("moneyline", "HT", home, r.get("cote 1")),
            _m("moneyline", "HT", "Draw", r.get("cote N")),
            _m("moneyline", "HT", away, r.get("cote 2")),
        ])
    elif table_key == "WinQ1":
        out.extend([
            _m("moneyline", "Q1", home, r.get("cote 1")),
            _m("moneyline", "Q1", "Draw", r.get("cote N")),
            _m("moneyline", "Q1", away, r.get("cote 2")),
        ])
    elif table_key == "WinQ2":
        out.extend([
            _m("moneyline", "Q2", home, r.get("cote 1")),
            _m("moneyline", "Q2", "Draw", r.get("cote N")),
            _m("moneyline", "Q2", away, r.get("cote 2")),
        ])
    else:
        per = _bb_period_for_total_key(table_key)
        if per:
            ln = _extract_line_from_cut(r.get("cut"))
            if ln:
                label_home_away = ""
                if table_key == "OUMatch1":
                    label_home_away = f"{home} "
                elif table_key == "OUMatch2":
                    label_home_away = f"{away} "
                elif table_key == "OUQ1_Home":
                    label_home_away = f"{home} "
                elif table_key == "OUQ1_Away":
                    label_home_away = f"{away} "
                elif table_key == "OUQ2_Home":
                    label_home_away = f"{home} "
                elif table_key == "OUQ2_Away":
                    label_home_away = f"{away} "
                out.extend([
                    _m("total", per, f"{label_home_away}Over {ln}", r.get("over")),
                    _m("total", per, f"{label_home_away}Under {ln}", r.get("under")),
                ])
        spr = _bb_spread_period(table_key)
        if spr:
            c1, c2 = r.get("cut 1"), r.get("cut 2")
            if c1 is not None and str(c1).strip():
                out.append(_m("spread", spr, f"{home} {c1}", r.get("cote 1")))
            if c2 is not None and str(c2).strip():
                out.append(_m("spread", spr, f"{away} {c2}", r.get("cote 2")))

    return [x for x in out if x is not None]


def _normalize_sport_mod(mod, table_handler, prop_row_fn=None) -> dict:
    buckets: dict[tuple, dict] = defaultdict(lambda: {"markets": [], "props": [], "url": ""})

    for table_key, df in mod.get_export_tables():
        if df is None or df.empty:
            continue
        # to_dict("records") évite iterrows() (beaucoup plus lent sur les gros tableaux).
        for row in df.to_dict(orient="records"):
            tour = str(row.get("tournoi") or "").strip() or "Autres"
            nom = str(row.get("nom du match") or "").strip()
            dfr = str(row.get("date du match") or "").strip()
            home, away = teams_from_match(nom)
            mk = (tour, nom, dfr)
            if not buckets[mk]["url"]:
                u = normalize_match_url(row.get("lien"))
                if u:
                    buckets[mk]["url"] = u
            if prop_row_fn:
                props = prop_row_fn(table_key, row)
                if props:
                    buckets[mk]["props"].extend(props)
            entries = table_handler(table_key, row, home, away)
            buckets[mk]["markets"].extend(entries)

    competitions: dict[str, list] = defaultdict(list)
    total_rows = 0
    total_matches = 0

    for (tour, nom, dfr), data in buckets.items():
        iso = fr_datetime_to_iso(dfr)
        competitions[tour].append({
            "match": nom,
            "date": iso,
            "url": data.get("url") or "",
            "markets": data["markets"],
            "props": data["props"],
        })
        total_matches += 1
        total_rows += len(data["markets"]) + len(data["props"])

    return {
        "total_rows": total_rows,
        "total_matches": total_matches,
        "competitions": dict(competitions),
    }


def build_output_json_document(tennis_mod, foot_mod, basket_mod, hockey_mod) -> dict:
    return {
        "generated_at": generated_at_output_format(),
        "sports": {
            "Tennis": _normalize_sport_mod(tennis_mod, _tennis_table_to_markets),
            "Football": _normalize_sport_mod(foot_mod, _foot_table_to_markets),
            "Basketball": _normalize_sport_mod(
                basket_mod, _basket_table_to_markets, _basket_prop_rows_from_df_row
            ),
            "Hockey": _normalize_sport_mod(
                hockey_mod, _hockey_table_to_markets, _hockey_prop_rows_from_df_row
            ),
        },
    }

"""
Scraping prématch hockey sur glace Unibet.fr — aligné sur scraping_specifications_clean.pdf (§ Hockey).

Couvert si présent sur Unibet :
  Moneyline RT (règlement 3 issues), ET (vainqueur prolongations / TAB),
  P1 / P2 / P3 (résultat, écarts, totaux, totaux par équipe, double chance, BTTS, 1er but, DNB),
  totaux buts RT / ET, handicaps 2 issues (y compris formulation « buts » ou paliers sans « buts »),
  props joueur (buteur, paliers points / passes, multi-buts, etc.).
"""

import asyncio
import json
import re

from unibet_event_link import link_from_event_payload
from unibet_http import unibet_aiohttp_get
from unibet_playwright import unibet_client_session
from datetime import datetime, timezone

import pandas as pd
import pytz

# Moneyline 3 issues (règlement)
WinRT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])
WinP1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])
WinP2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])
WinP3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])

# Moneyline 2 issues (prorogation / TAB)
WinET = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote 2"])

# DNB (remboursé si nul)
DNB_RT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote 2"])
DNB_P1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote 2"])
DNB_P2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote 2"])
DNB_P3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote 2"])

# Totaux O/U
OURT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUET = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUHome_RT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUAway_RT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP1_Home = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP1_Away = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP2_Home = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP2_Away = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP3_Home = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])
OUP3_Away = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "over", "under"])

# Handicaps 2 issues (puck line–like)
HDP_OTPI = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut 1", "cut 2", "cote 1", "cote 2"])
HDP_OTSO = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut 1", "cut 2", "cote 1", "cote 2"])
HDP_P1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut 1", "cut 2", "cote 1", "cote 2"])
HDP_P2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut 1", "cut 2", "cote 1", "cote 2"])
HDP_P3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut 1", "cut 2", "cote 1", "cote 2"])

# Double chance (3 sélections Unibet)
C2_RT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "1N", "12", "N2"])
C2_P1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "1N", "12", "N2"])
C2_P2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "1N", "12", "N2"])
C2_P3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "1N", "12", "N2"])

# BTTS
BTTS_RT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "oui", "non"])
BTTS_P1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "oui", "non"])
BTTS_P2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "oui", "non"])
BTTS_P3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "oui", "non"])

# 1er but
FirstGoal_RT = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])
FirstGoal_P1 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])
FirstGoal_P2 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])
FirstGoal_P3 = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "cote 1", "cote N", "cote 2"])

# Prolongations Oui/Non
OT_YESNO = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "cut", "oui", "non"])

PropsHockey = pd.DataFrame(columns=["nom du match", "date du match", "tournoi", "lien", "player", "stat", "selection", "odds"])

UNIBET_SPORT_NODE_ID = 703696097


def get_export_tables():
    return [
        ("WinRT", WinRT),
        ("WinET", WinET),
        ("WinP1", WinP1),
        ("WinP2", WinP2),
        ("WinP3", WinP3),
        ("DNB_RT", DNB_RT),
        ("DNB_P1", DNB_P1),
        ("DNB_P2", DNB_P2),
        ("DNB_P3", DNB_P3),
        ("OURT", OURT),
        ("OUET", OUET),
        ("OUHome_RT", OUHome_RT),
        ("OUAway_RT", OUAway_RT),
        ("OUP1", OUP1),
        ("OUP2", OUP2),
        ("OUP3", OUP3),
        ("OUP1_Home", OUP1_Home),
        ("OUP1_Away", OUP1_Away),
        ("OUP2_Home", OUP2_Home),
        ("OUP2_Away", OUP2_Away),
        ("OUP3_Home", OUP3_Home),
        ("OUP3_Away", OUP3_Away),
        ("HDP_OTPI", HDP_OTPI),
        ("HDP_OTSO", HDP_OTSO),
        ("HDP_P1", HDP_P1),
        ("HDP_P2", HDP_P2),
        ("HDP_P3", HDP_P3),
        ("C2_RT", C2_RT),
        ("C2_P1", C2_P1),
        ("C2_P2", C2_P2),
        ("C2_P3", C2_P3),
        ("BTTS_RT", BTTS_RT),
        ("BTTS_P1", BTTS_P1),
        ("BTTS_P2", BTTS_P2),
        ("BTTS_P3", BTTS_P3),
        ("FirstGoal_RT", FirstGoal_RT),
        ("FirstGoal_P1", FirstGoal_P1),
        ("FirstGoal_P2", FirstGoal_P2),
        ("FirstGoal_P3", FirstGoal_P3),
        ("OT_YESNO", OT_YESNO),
        ("PropsHockey", PropsHockey),
    ]


def reset_dataframes():
    for _, df in get_export_tables():
        df.drop(df.index, inplace=True)


def datage(timestamp_millisecondes):
    try:
        ts = round(timestamp_millisecondes / 1000)
        date = datetime.fromtimestamp(ts, tz=timezone.utc)
        date = date.astimezone(pytz.timezone("Europe/Paris"))
        return date.strftime("%d/%m/%Y %H:%M:%S")
    except Exception as e:
        print(f"Erreur lors de la conversion de date: {e}")
        return None


def calculate_odd(selection):
    try:
        cpu = int(selection.get("currentPriceUp", 0))
        cpd = int(selection.get("currentPriceDown", 0))
        if cpd == 0:
            return None
        return round(1 + cpu / cpd, 2)
    except (ValueError, TypeError, AttributeError):
        return None


def hockey_spread_cut_from_label(s):
    """Convertit les libellés d'écart Unibet hockey en handicap style « +/-X.X »."""
    if s is None or not isinstance(s, str):
        return None
    m = re.search(r"gagne de (\d+) buts? ou \+", s, re.I)
    if m:
        x = int(m.group(1))
        return "-" + str(x - 0.5)
    m = re.search(r"gagne de (\d+) ou \+", s, re.I)
    if m:
        x = int(m.group(1))
        return "-" + str(x - 0.5)
    m = re.search(r"ne perd pas ou perd de (\d+) buts? ou -", s, re.I)
    if m:
        x = int(m.group(1))
        return "+" + str(x + 0.5)
    m = re.search(r"ne perd pas ou perd de (\d+) ou -", s, re.I)
    if m:
        x = int(m.group(1))
        return "+" + str(x + 0.5)
    if re.search(r"ne perd pas\s*$", s, re.I):
        return "+0.5"
    return None


def pair_ou_by_threshold(selections):
    unders, overs = {}, {}
    for s in selections:
        name = s.get("name") or ""
        mu = re.search(r"Moins de ([\d,]+)", name, re.I)
        mo = re.search(r"Plus de ([\d,]+)", name, re.I)
        if mu:
            unders[mu.group(1).replace(",", ".")] = s
        if mo:
            overs[mo.group(1).replace(",", ".")] = s
    pairs = []
    for k in sorted(unders.keys(), key=lambda x: float(x)):
        if k in overs:
            pairs.append((k, unders[k], overs[k]))
    return pairs


def append_ou_market(df, nom, date_str, tournoi, link, y):
    selections = y.get("selections") or []
    market_type = y.get("marketType")
    if not selections or not market_type:
        return
    pairs = pair_ou_by_threshold(selections)
    if not pairs and len(selections) >= 2:
        a, b = selections[0], selections[1]
        na, nb = (a.get("name") or "").lower(), (b.get("name") or "").lower()
        if "moins" in na and "plus" in nb:
            pairs = [("line", a, b)]
        elif "plus" in na and "moins" in nb:
            pairs = [("line", b, a)]
        else:
            pairs = [("line", a, b)]
    for cut_k, su, so in pairs:
        u_odd = calculate_odd(su)
        o_odd = calculate_odd(so)
        if cut_k == "line":
            if u_odd is None and o_odd is None:
                continue
        elif not u_odd or not o_odd:
            continue
        if cut_k == "line":
            cut = market_type
        elif len(pairs) > 1:
            cut = f"{market_type} — {cut_k}"
        else:
            cut = market_type
        df.loc[len(df)] = {
            "nom du match": nom,
            "date du match": date_str,
            "tournoi": tournoi,
            "lien": link,
            "cut": cut,
            "over": o_odd,
            "under": u_odd,
        }


def append_three_way(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 3:
        return
    o1, o2, o3 = calculate_odd(sel[0]), calculate_odd(sel[1]), calculate_odd(sel[2])
    if o1 and o2 and o3:
        df.loc[len(df)] = {
            "nom du match": nom,
            "date du match": date_str,
            "tournoi": tournoi,
            "lien": link,
            "cut": y.get("marketType"),
            "cote 1": o1,
            "cote N": o2,
            "cote 2": o3,
        }


def append_two_way_ml(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 2:
        return
    o1, o2 = calculate_odd(sel[0]), calculate_odd(sel[1])
    if o1 and o2:
        df.loc[len(df)] = {
            "nom du match": nom,
            "date du match": date_str,
            "tournoi": tournoi,
            "lien": link,
            "cut": y.get("marketType"),
            "cote 1": o1,
            "cote 2": o2,
        }


def append_double_chance(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 3:
        return
    o1, o2, o3 = calculate_odd(sel[0]), calculate_odd(sel[1]), calculate_odd(sel[2])
    if o1 and o2 and o3:
        df.loc[len(df)] = {
            "nom du match": nom,
            "date du match": date_str,
            "tournoi": tournoi,
            "lien": link,
            "1N": o1,
            "12": o2,
            "N2": o3,
        }


def append_yes_no(df, nom, date_str, tournoi, link, y):
    selections = y.get("selections") or []
    if len(selections) != 2:
        return
    n0 = (selections[0].get("name") or "").lower()
    n1 = (selections[1].get("name") or "").lower()
    o0, o1 = calculate_odd(selections[0]), calculate_odd(selections[1])
    if not o0 or not o1:
        return
    if "oui" in n0 and "non" in n1:
        oui, non = o0, o1
    elif "non" in n0 and "oui" in n1:
        oui, non = o1, o0
    else:
        oui, non = o0, o1
    df.loc[len(df)] = {
        "nom du match": nom,
        "date du match": date_str,
        "tournoi": tournoi,
        "lien": link,
        "cut": y.get("marketType"),
        "oui": oui,
        "non": non,
    }


def append_spread_2way(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 2:
        return
    c1 = hockey_spread_cut_from_label(sel[0].get("name"))
    c2 = hockey_spread_cut_from_label(sel[1].get("name"))
    o1, o2 = calculate_odd(sel[0]), calculate_odd(sel[1])
    if o1 and o2:
        df.loc[len(df)] = {
            "nom du match": nom,
            "date du match": date_str,
            "tournoi": tournoi,
            "lien": link,
            "cut 1": c1,
            "cut 2": c2,
            "cote 1": o1,
            "cote 2": o2,
        }


def append_hockey_list_props(df, nom, date_str, tournoi, link, y, stat, selection_value):
    for s in y.get("selections") or []:
        player = (s.get("name") or "").strip()
        if not player or player.lower() == "aucun":
            continue
        odd = calculate_odd(s)
        if not odd:
            continue
        df.loc[len(df)] = {
            "nom du match": nom,
            "date du match": date_str,
            "tournoi": tournoi,
            "lien": link,
            "player": player,
            "stat": stat,
            "selection": selection_value,
            "odds": odd,
        }


def process_market(nom, date_str, tournoi, link, y):
    mn = y.get("marketName")
    if not mn:
        return

    if mn == "Résultat du match":
        append_three_way(WinRT, nom, date_str, tournoi, link, y)
        return
    if mn in ("Vainqueur (Prolongations et tirs aux buts inclus)", "Vainqueur du match (Prolongations incluses)"):
        append_two_way_ml(WinET, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Tiers-temps - Résultat":
        append_three_way(WinP1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Tiers-temps - Résultat":
        append_three_way(WinP2, nom, date_str, tournoi, link, y)
        return
    if mn == "3e Tiers-temps - Résultat":
        append_three_way(WinP3, nom, date_str, tournoi, link, y)
        return

    if mn == "Résultat du match (remboursé si match nul)":
        append_two_way_ml(DNB_RT, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Tiers-temps - Résultat (remboursé si match nul)":
        append_two_way_ml(DNB_P1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Tiers-temps - Résultat (remboursé si match nul)":
        append_two_way_ml(DNB_P2, nom, date_str, tournoi, link, y)
        return
    if mn == "3e Tiers-temps - Résultat (remboursé si match nul)":
        append_two_way_ml(DNB_P3, nom, date_str, tournoi, link, y)
        return

    ou_map = {
        "Total de buts": OURT,
        "Total de buts (Prolongations et tirs aux buts inclus)": OUET,
        "Total de buts - Equipe à domicile": OUHome_RT,
        "Total de buts - Equipe à l'extérieur": OUAway_RT,
        "1er Tiers-temps - Total de buts": OUP1,
        "2e Tiers-temps - Total de buts": OUP2,
        "3e Tiers-temps - Total de buts": OUP3,
        "1er Tiers-temps - Total de buts - Equipe à domicile": OUP1_Home,
        "1er Tiers-temps - Total de buts - Equipe à l'extérieur": OUP1_Away,
        "2e Tiers-temps - Total de buts - Equipe à domicile": OUP2_Home,
        "2e Tiers-temps - Total de buts - Equipe à l'extérieur": OUP2_Away,
        "3e Tiers-temps - Total de buts - Equipe à domicile": OUP3_Home,
        "3e Tiers-temps - Total de buts - Equipe à l'extérieur": OUP3_Away,
    }
    if mn in ou_map:
        append_ou_market(ou_map[mn], nom, date_str, tournoi, link, y)
        return

    if mn == "Ecart entre les équipes (Prolongations incluses)":
        append_spread_2way(HDP_OTPI, nom, date_str, tournoi, link, y)
        return
    if mn == "Ecart entre équipes - 2 possibilités (Prolongations et tirs aux buts inclus)":
        append_spread_2way(HDP_OTSO, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Tiers-temps - Ecart entre équipes (2 possibilités)":
        append_spread_2way(HDP_P1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Tiers-temps - Ecart entre équipes (2 possibilités)":
        append_spread_2way(HDP_P2, nom, date_str, tournoi, link, y)
        return
    if mn == "3e Tiers-temps - Ecart entre équipes (2 possibilités)":
        append_spread_2way(HDP_P3, nom, date_str, tournoi, link, y)
        return

    if mn == "Double chance":
        append_double_chance(C2_RT, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Tiers-temps - Double Chance":
        append_double_chance(C2_P1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Tiers-temps - Double chance":
        append_double_chance(C2_P2, nom, date_str, tournoi, link, y)
        return
    if mn == "3e Tiers-temps - Double chance":
        append_double_chance(C2_P3, nom, date_str, tournoi, link, y)
        return

    if mn == "But pour les 2 équipes":
        append_yes_no(BTTS_RT, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Tiers-temps - But pour les 2 équipes":
        append_yes_no(BTTS_P1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Tiers-temps - But pour les 2 équipes":
        append_yes_no(BTTS_P2, nom, date_str, tournoi, link, y)
        return
    if mn == "3e Tiers-temps - But pour les 2 équipes":
        append_yes_no(BTTS_P3, nom, date_str, tournoi, link, y)
        return

    if mn == "Qui marquera le 1er but":
        append_three_way(FirstGoal_RT, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Tiers-temps - Qui marquera le 1er but":
        append_three_way(FirstGoal_P1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Tiers-temps - Qui marquera le 1er but":
        append_three_way(FirstGoal_P2, nom, date_str, tournoi, link, y)
        return
    if mn == "3e Tiers-temps - Qui marquera le 1er but":
        append_three_way(FirstGoal_P3, nom, date_str, tournoi, link, y)
        return

    if mn == "Prolongations Oui/Non":
        append_yes_no(OT_YESNO, nom, date_str, tournoi, link, y)
        return

    if mn == "Buteur (Prolongations incluses)":
        append_hockey_list_props(PropsHockey, nom, date_str, tournoi, link, y, "Goals", "Anytime")
        return
    if mn == "Buteur et son équipe gagne (Prolongations incluses)":
        append_hockey_list_props(PropsHockey, nom, date_str, tournoi, link, y, "Goals", "To score & team win")
        return
    if mn == "Buteur et son équipe gagne de 2 buts ou +":
        append_hockey_list_props(PropsHockey, nom, date_str, tournoi, link, y, "Goals", "To score & team win by 2+")
        return

    m_pts = re.match(r"Joueur inscrit (\d+) points? ou plus", mn, re.I)
    if m_pts:
        n = m_pts.group(1)
        append_hockey_list_props(PropsHockey, nom, date_str, tournoi, link, y, "Points", f"{n}+")
        return
    m_ast = re.match(r"Joueur réalisant (\d+) passes? ou plus", mn, re.I)
    if m_ast:
        n = m_ast.group(1)
        append_hockey_list_props(PropsHockey, nom, date_str, tournoi, link, y, "Assists", f"{n}+")
        return
    m_g2 = re.match(r"Le joueur marque (\d+) buts? ou \+ \(Prolongations incluses\)", mn, re.I)
    if m_g2:
        n = m_g2.group(1)
        append_hockey_list_props(PropsHockey, nom, date_str, tournoi, link, y, "Goals", f"{n}+")
        return


async def fetch(session, url):
    get_text = getattr(session, "get_text", None)
    if get_text is not None:
        return await get_text(url)
    return await unibet_aiohttp_get(session, url)


async def fetch_event_data(session, event_id):
    url = f"https://www.unibet.fr/zones/event.json?eventId={event_id}"
    response = await fetch(session, url)
    if response is None:
        return None
    try:
        return json.loads(response)
    except json.JSONDecodeError as e:
        print(f"Erreur de décodage JSON pour l'événement {event_id}: {e}")
        return None


async def run_scrape():
    reset_dataframes()
    TM = []
    url = (
        f"https://www.unibet.fr/zones/v3/sportnode/markets.json?"
        f"nodeId={UNIBET_SPORT_NODE_ID}&filter=R%25C3%25A9sultat&marketname=R%25C3%25A9sultat%2520du%2520match"
    )
    async with unibet_client_session() as session:
        response = await fetch(session, url)
        if response is None:
            print("Impossible de récupérer les données initiales (hockey)")
            return
        try:
            requetejson = json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Erreur de décodage JSON (hockey): {e}")
            return

        markets_by_type = requetejson.get("marketsByType")
        if not markets_by_type:
            print("Aucun marché hockey trouvé")
            return
        days = markets_by_type[0].get("days")
        if not days:
            print("Aucun jour hockey trouvé")
            return

        for day in days:
            for event in day.get("events", []):
                tournoi = event.get("competitionName")
                date = event.get("eventStartDate")
                title = event.get("eventName")
                event_id = event.get("eventId")
                markets = event.get("markets", [])
                if markets:
                    link = "unibet.fr" + markets[0].get("eventFriendlyUrl", "")
                    TM.append([event_id, title, tournoi, date, link])

        if not TM:
            print("Aucun événement hockey trouvé")
            return

        semaphore = asyncio.Semaphore(14)

        async def fetch_with_semaphore(event_id):
            async with semaphore:
                return await fetch_event_data(session, event_id)

        event_data_list = await asyncio.gather(*[fetch_with_semaphore(eid) for eid, _, _, _, _ in TM])

        for event_data, (_, nom, tournoi, date, link) in zip(event_data_list, TM):
            if event_data is None:
                continue
            link = link_from_event_payload(event_data, link)
            date_str = datage(date)
            if date_str is None:
                continue
            for bloc in event_data.get("marketClassList", []):
                for y in bloc.get("marketList", []):
                    if not y.get("selections"):
                        continue
                    process_market(nom, date_str, tournoi, link, y)


async def main():
    await run_scrape()
    print("\n=== Résultats hockey (spec PDF + marchés Unibet listés) ===")
    for label, df in get_export_tables():
        print(f"{label}: {len(df)} lignes")


if __name__ == "__main__":
    asyncio.run(main())

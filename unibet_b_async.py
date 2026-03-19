"""
Scraping prématch basket Unibet.fr — aligné sur scraping_specifications_clean.pdf (§ Basketball).

Couvert si présent sur Unibet :
  Moneyline Q1 / Q2 / mi-temps / match (3 issues ou 2 issues selon marché),
  écart points Q1 / Q2 / MT / match (prolongations incluses pour le match quand indiqué),
  totaux points Q1 / Q2 / MT / match, totaux par équipe Q1/Q2/match quand dispo.
  Props joueur NBA/Euro/etc. : paliers Over/Under (points, PR, PA, PRA, 3PM, rebonds, passes),
  double double / triple double (liste de joueurs Unibet), voir scraping_specifications_clean.pdf § NBA.
"""

import aiohttp
import asyncio
import json
import re

from unibet_event_link import link_from_event_payload
from unibet_http import UNIBET_REQUEST_HEADERS, unibet_connector, unibet_trust_env, warm_unibet_session
from datetime import datetime, timezone

import pandas as pd
import pytz

# Moneyline
Win = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote 2'])
WinHP = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
WinHT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
WinQ1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
WinQ2 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])

# Totaux O/U (over = Plus, under = Moins)
OUMatch = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUHT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUQ1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUQ2 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUMatch1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUMatch2 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUQ1_Home = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUQ1_Away = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUQ2_Home = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUQ2_Away = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])

# Handicap 2 issues
HDPPoints = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])
HDPHT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])
HDPQ1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])
HDPQ2 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])

# DNB match (2 issues)
DNB_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote 2'])

# Props joueur (schéma export output.json : player, stat, selection, odds)
PropsNBA = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'player', 'stat', 'selection', 'odds'])

UNIBET_SPORT_NODE_ID = 703696076

# Libellé Unibet.fr -> libellé stat proche de output.json / Pinnacle
NBA_PLAYER_OU_MARKETS = {
    "Nombre de points marqués par le joueur": "Total Points",
    "Performance du joueur (Points + Rebonds + Passes)": "Total Pts+Reb+Ast",
    "Performance du Joueur (Points + Rebonds)": "Total Pts+Reb",
    "Performance du Joueur (Points + Passes)": "Total Pts+Ast",
    "Performance du Joueur (Passes + Rebonds)": "Total Ast+Reb",
    "Nombre de paniers à 3 points du joueur": "Total 3PM",
    "Nombre de rebonds du joueur": "Total Rebounds",
    "Nombre de passes du joueur": "Total Assists",
}

_PLAYER_OU_SELECTION = re.compile(
    r"^(.+?)\s*-\s*(Moins|Plus)\s+de\s+([\d,]+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)


def get_export_tables():
    return [
        ("Win", Win),
        ("WinHP", WinHP),
        ("WinHT", WinHT),
        ("WinQ1", WinQ1),
        ("WinQ2", WinQ2),
        ("DNB_FT", DNB_FT),
        ("OUMatch", OUMatch),
        ("OUHT", OUHT),
        ("OUQ1", OUQ1),
        ("OUQ2", OUQ2),
        ("OUMatch1", OUMatch1),
        ("OUMatch2", OUMatch2),
        ("OUQ1_Home", OUQ1_Home),
        ("OUQ1_Away", OUQ1_Away),
        ("OUQ2_Home", OUQ2_Home),
        ("OUQ2_Away", OUQ2_Away),
        ("HDPPoints", HDPPoints),
        ("HDPHT", HDPHT),
        ("HDPQ1", HDPQ1),
        ("HDPQ2", HDPQ2),
        ("PropsNBA", PropsNBA),
    ]


def reset_dataframes():
    for _, df in get_export_tables():
        df.drop(df.index, inplace=True)


def datage(timestamp_millisecondes):
    try:
        ts = round(timestamp_millisecondes / 1000)
        date = datetime.fromtimestamp(ts, tz=timezone.utc)
        date = date.astimezone(pytz.timezone('Europe/Paris'))
        return date.strftime("%d/%m/%Y %H:%M:%S")
    except Exception as e:
        print(f"Erreur lors de la conversion de date: {e}")
        return None


def contains_specific_pattern_points(s):
    if s is None:
        return None
    pattern1 = r'gagne de (\d+) ou +'
    pattern2 = r'ne perd pas ou perd de (\d+)'
    match1 = re.search(pattern1, s)
    match2 = re.search(pattern2, s)
    if match1:
        x = int(match1.group(1))
        return "-" + str(x - 0.5)
    if match2:
        x = int(match2.group(1))
        return "+" + str(x + 0.5)
    # libellés "ou -" fréquents sur écarts quart-temps
    m3 = re.search(r'gagne de (\d+) ou \+', s)
    if m3:
        x = int(m3.group(1))
        return "-" + str(x - 0.5)
    m4 = re.search(r'ne perd pas ou perd de (\d+) ou -', s)
    if m4:
        x = int(m4.group(1))
        return "+" + str(x + 0.5)
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
        if not u_odd or not o_odd:
            continue
        if cut_k == "line":
            cut = market_type
        elif len(pairs) > 1:
            cut = f"{market_type} — {cut_k}"
        else:
            cut = market_type
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cut': cut, 'over': o_odd, 'under': u_odd,
        }


def append_three_way(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 3:
        return
    o1, o2, o3 = calculate_odd(sel[0]), calculate_odd(sel[1]), calculate_odd(sel[2])
    if o1 and o2 and o3:
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cut': y.get("marketType"),
            'cote 1': o1, 'cote N': o2, 'cote 2': o3,
        }


def append_two_way_ml(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 2:
        return
    o1, o2 = calculate_odd(sel[0]), calculate_odd(sel[1])
    if o1 and o2:
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cut': y.get("marketType"),
            'cote 1': o1, 'cote 2': o2,
        }


def append_nba_player_yes_props(df, nom, date_str, tournoi, link, y, stat_label):
    """Marchés type « Joueur réalisant un double double » : une cote par joueur."""
    for s in y.get("selections") or []:
        player = (s.get("name") or "").strip()
        if not player:
            continue
        odd = calculate_odd(s)
        if not odd:
            continue
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'player': player, 'stat': stat_label, 'selection': 'Yes', 'odds': odd,
        }


def append_nba_player_ou_props(df, nom, date_str, tournoi, link, y, stat_label):
    """Sélections type «Joueur - Moins de 7,5» / «Joueur - Plus de 7,5»."""
    selections = y.get("selections") or []
    by_pair: dict[tuple[str, str], dict] = {}
    for s in selections:
        name = (s.get("name") or "").strip()
        m = _PLAYER_OU_SELECTION.match(name)
        if not m:
            continue
        player, side_raw, line = m.group(1).strip(), m.group(2).lower(), m.group(3).replace(",", ".")
        side = "moins" if "moins" in side_raw else "plus"
        key = (player, line)
        if key not in by_pair:
            by_pair[key] = {}
        by_pair[key][side] = s
    for (player, line), sides in by_pair.items():
        su, so = sides.get("moins"), sides.get("plus")
        if so:
            o_odd = calculate_odd(so)
            if o_odd:
                df.loc[len(df)] = {
                    'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
                    'player': player, 'stat': stat_label, 'selection': f"Over {line}", 'odds': o_odd,
                }
        if su:
            u_odd = calculate_odd(su)
            if u_odd:
                df.loc[len(df)] = {
                    'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
                    'player': player, 'stat': stat_label, 'selection': f"Under {line}", 'odds': u_odd,
                }


def append_spread_2way(df, nom, date_str, tournoi, link, y):
    sel = y.get("selections") or []
    if len(sel) < 2:
        return
    c1 = contains_specific_pattern_points(sel[0].get("name"))
    c2 = contains_specific_pattern_points(sel[1].get("name"))
    o1, o2 = calculate_odd(sel[0]), calculate_odd(sel[1])
    if o1 and o2:
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cut 1': c1, 'cut 2': c2, 'cote 1': o1, 'cote 2': o2,
        }


def process_market(nom, date_str, tournoi, link, y):
    mn = y.get("marketName")
    if not mn:
        return

    if mn == "Résultat du match":
        append_three_way(WinHP, nom, date_str, tournoi, link, y)
        return
    if mn == "Vainqueur (Prolongations incluses)":
        append_two_way_ml(Win, nom, date_str, tournoi, link, y)
        return
    if mn == "1ère Mi-temps - Résultat":
        append_three_way(WinHT, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Quart-temps - Résultat":
        append_three_way(WinQ1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Quart-temps - Résultat":
        append_three_way(WinQ2, nom, date_str, tournoi, link, y)
        return
    if mn == "Résultat du match (remboursé si match nul)":
        append_two_way_ml(DNB_FT, nom, date_str, tournoi, link, y)
        return

    stat_label = NBA_PLAYER_OU_MARKETS.get(mn)
    if stat_label:
        append_nba_player_ou_props(PropsNBA, nom, date_str, tournoi, link, y, stat_label)
        return
    if mn == "Joueur réalisant un double double":
        append_nba_player_yes_props(PropsNBA, nom, date_str, tournoi, link, y, "Double Double")
        return
    if mn == "Joueur réalisant un triple double":
        append_nba_player_yes_props(PropsNBA, nom, date_str, tournoi, link, y, "Triple Double")
        return

    ou_map = {
        "Total de points (Prolongations incluses)": OUMatch,
        "1ère Mi-temps - Total de points": OUHT,
        "1er Quart-temps - Total de points": OUQ1,
        "2e Quart-temps - Total de points": OUQ2,
        "Total de points - Equipe à domicile (Prolongations incluses)": OUMatch1,
        "Total de points - Equipe à l'extérieur (Prolongations incluses)": OUMatch2,
        "1er Quart-temps - Total de points - Equipe à domicile": OUQ1_Home,
        "1er Quart-temps - Total de points - Equipe à l'extérieur": OUQ1_Away,
        "2e Quart-temps - Total de points - Equipe à domicile": OUQ2_Home,
        "2e Quart-temps - Total de points - Equipe à l'extérieur": OUQ2_Away,
    }
    if mn in ou_map:
        append_ou_market(ou_map[mn], nom, date_str, tournoi, link, y)
        return

    if mn == "Ecart entre les équipes (Prolongations incluses)":
        append_spread_2way(HDPPoints, nom, date_str, tournoi, link, y)
        return
    if mn in ("1ère Mi-temps - Ecart entre les équipes", "1ère Mi-temps - Ecart entre les équipes (2 possibilités)"):
        append_spread_2way(HDPHT, nom, date_str, tournoi, link, y)
        return
    if mn == "1er Quart-temps - Ecart entre les équipes":
        append_spread_2way(HDPQ1, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Quart-temps - Ecart entre les équipes":
        append_spread_2way(HDPQ2, nom, date_str, tournoi, link, y)
        return


async def fetch(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientResponseError as e:
        print(f"Erreur HTTP {e.status} lors de la récupération de {url}: {e.message}")
        return None
    except asyncio.TimeoutError:
        print(f"Timeout lors de la récupération de {url}")
        return None
    except Exception as e:
        print(f"Erreur lors de la récupération de {url}: {e}")
        return None


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
    url = f"https://www.unibet.fr/zones/v3/sportnode/markets.json?nodeId={UNIBET_SPORT_NODE_ID}&filter=R%25C3%25A9sultat&marketname=Vainqueur%2520(Prolongations%2520incluses)"

    connector = unibet_connector()
    async with aiohttp.ClientSession(
        headers=UNIBET_REQUEST_HEADERS,
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=30),
        trust_env=unibet_trust_env(),
    ) as session:
        await warm_unibet_session(session)
        response = await fetch(session, url)
        if response is None:
            print("Impossible de récupérer les données initiales")
            return
        try:
            requetejson = json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Erreur de décodage JSON: {e}")
            return

        marketsByType = requetejson.get("marketsByType")
        if not marketsByType:
            print("Aucun marché trouvé")
            return
        days = marketsByType[0].get("days")
        if not days:
            print("Aucun jour trouvé")
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
            print("Aucun événement trouvé")
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

    frames = [
        ("Win (moneyline OT inclus)", Win),
        ("WinHP (résultat 3-voies règlement)", WinHP),
        ("WinHT (moneyline MT)", WinHT),
        ("WinQ1", WinQ1),
        ("WinQ2", WinQ2),
        ("DNB_FT", DNB_FT),
        ("OUMatch (total points match OT)", OUMatch),
        ("OUHT", OUHT),
        ("OUQ1", OUQ1),
        ("OUQ2", OUQ2),
        ("OUMatch1 (total dom OT)", OUMatch1),
        ("OUMatch2 (total ext OT)", OUMatch2),
        ("OUQ1_Home", OUQ1_Home),
        ("OUQ1_Away", OUQ1_Away),
        ("OUQ2_Home", OUQ2_Home),
        ("OUQ2_Away", OUQ2_Away),
        ("HDPPoints (écart match OT)", HDPPoints),
        ("HDPHT (écart MT)", HDPHT),
        ("HDPQ1", HDPQ1),
        ("HDPQ2", HDPQ2),
        ("PropsNBA (joueur O/U)", PropsNBA),
    ]
    print("\n=== Résultats basket (spec PDF) ===")
    for label, df in frames:
        print(f"{label}: {len(df)} lignes")
    for _, df in frames:
        print(df)


if __name__ == "__main__":
    asyncio.run(main())

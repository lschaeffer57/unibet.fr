"""
Scraping prématch tennis Unibet.fr, aligné sur scraping_specifications_clean.pdf (§ Tennis).

Marchés visés (ignorés si absents sur Unibet) :
  - Moneyline match / 1er set
  - Handicap en jeux match / set 1 (si Unibet les affiche sous un nom détecté)
  - Handicap en sets (Écart de sets)
  - Total jeux (match, set 1)
  - Total jeux joueur A/B (match, set 1)

Football, basket, hockey et props joueurs NBA du même PDF : voir unibet_f_async.py,
unibet_b_async.py ; hockey / props non présents dans ce fichier.
Export JSON (schéma type output.json : sports / competitions / match / markets) : unibet_all_json.py.
"""

import aiohttp
import asyncio
import json
from datetime import datetime, timezone
import pandas as pd
import pytz
import re

from unibet_event_link import link_from_event_payload

# --- DataFrames (spec tennis + extra utile) ---
OUJEUSet1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUJEU = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUJoueur1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUJoueur2 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUJoueur1Set1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUJoueur2Set1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
Win = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cote 1', 'cote 2'])
Winset1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cote 1', 'cote 2'])
Totalset = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cote 1', 'cote 2'])
HDPSet = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])
HDJeuxFT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])
HDJeuxSet1 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut 1', 'cut 2', 'cote 1', 'cote 2'])

# Noms supplémentaires possibles pour handicap en jeux (étendre si Unibet change les libellés)
JEUX_HANDICAP_FT_MARKET_NAMES = frozenset({
    "Handicap en jeux",
    "Match - Handicap en jeux",
    "Jeux avec handicap",
})
JEUX_HANDICAP_SET1_MARKET_NAMES = frozenset({
    "Set 1 - Handicap en jeux",
    "Set 1 - Jeux avec handicap",
})

UNIBET_SPORT_NODE_ID = 703696075


def get_export_tables():
    """(clé JSON, DataFrame) pour export agrégé — garder l’ordre stable."""
    return [
        ("OUJEUSet1", OUJEUSet1),
        ("OUJEU", OUJEU),
        ("OUJoueur1", OUJoueur1),
        ("OUJoueur2", OUJoueur2),
        ("OUJoueur1Set1", OUJoueur1Set1),
        ("OUJoueur2Set1", OUJoueur2Set1),
        ("Win", Win),
        ("Winset1", Winset1),
        ("Totalset", Totalset),
        ("HDPSet", HDPSet),
        ("HDJeuxFT", HDJeuxFT),
        ("HDJeuxSet1", HDJeuxSet1),
    ]


def reset_dataframes():
    for _, df in get_export_tables():
        df.drop(df.index, inplace=True)


def datage(timestamp_millisecondes):
    try:
        timestamp_secondes = round(timestamp_millisecondes / 1000)
        date = datetime.fromtimestamp(timestamp_secondes, tz=timezone.utc)
        tz_france = pytz.timezone('Europe/Paris')
        date = date.astimezone(tz_france)
        date_format_francais = date.strftime("%d/%m/%Y %H:%M:%S")
        return date_format_francais
    except Exception as e:
        print(f"Erreur lors de la conversion de date: {e}")
        return None


def contains_specific_pattern_set(s):
    if s is None:
        return None
    pattern1 = r'gagne de (\d+) ou +'
    pattern2 = r'ne perd pas ou perd de (\d+)'
    match1 = re.search(pattern1, s)
    match2 = re.search(pattern2, s)
    if match1:
        x = int(match1.group(1))
        x = x - 0.5
        return "-" + str(x)
    elif match2:
        x = int(match2.group(1))
        x = x + 0.5
        return "+" + str(x)
    else:
        return None


def contains_specific_pattern_games(s):
    """Libellés possibles pour handicap en jeux (même logique que les sets si Unibet les aligne)."""
    if s is None:
        return None
    for pat in (r'gagne de (\d+) jeux?', r'gagne de (\d+) ou \+ jeux?'):
        m = re.search(pat, s, re.I)
        if m:
            return "-" + str(int(m.group(1)) - 0.5)
    m2 = re.search(r'ne perd pas ou perd de (\d+) jeux?', s, re.I)
    if m2:
        return "+" + str(int(m2.group(1)) + 0.5)
    return contains_specific_pattern_set(s)


def pair_ou_by_threshold(selections):
    """Associe Moins / Plus pour chaque ligne (ex. paliers multiples dans un seul marché)."""
    unders = {}
    overs = {}
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
    """Remplit over = cote Plus, under = cote Moins. Une ligne par palier."""
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
        under_odd = calculate_odd(su)
        over_odd = calculate_odd(so)
        if not under_odd or not over_odd:
            continue
        if cut_k == "line":
            cut = market_type
        elif len(pairs) > 1:
            cut = f"{market_type} — {cut_k}"
        else:
            cut = market_type
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cut': cut, 'over': over_odd, 'under': under_odd,
        }


def append_two_way_moneyline(df, nom, date_str, tournoi, link, selections):
    if len(selections) < 2:
        return
    o1 = calculate_odd(selections[0])
    o2 = calculate_odd(selections[1])
    if o1 and o2:
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cote 1': o1, 'cote 2': o2,
        }


def append_multi_way_first_two_as_moneyline(df, nom, date_str, tournoi, link, y, min_selections=2):
    """Pour 'Nombre exact de sets' : garde les deux premières cotes si le marché est multi-sélection."""
    selections = y.get("selections") or []
    if len(selections) < min_selections:
        return
    append_two_way_moneyline(df, nom, date_str, tournoi, link, selections)


def calculate_odd(selection):
    try:
        cpu = int(selection.get("currentPriceUp", 0))
        cpd = int(selection.get("currentPriceDown", 0))
        if cpd == 0:
            return None
        return round(1 + cpu / cpd, 2)
    except (ValueError, TypeError, AttributeError):
        return None


async def fetch(session, url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.unibet.fr/',
            'Origin': 'https://www.unibet.fr',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
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


def process_market_row(nom, date_str, tournoi, link, y):
    """Dispatch un marché Unibet vers le DataFrame spec tennis."""
    market_name = y.get("marketName")
    selections = y.get("selections") or []

    ou_routes = {
        "Set 1 - Nombre de jeux": OUJEUSet1,
        "Nombre de jeux": OUJEU,
        "Joueur 1 - Nombre de jeux": OUJoueur1,
        "Joueur 2 - Nombre de jeux": OUJoueur2,
        "Set 1 - Nombre de jeux du joueur 1": OUJoueur1Set1,
        "Set 1 - Nombre de jeux du joueur 2": OUJoueur2Set1,
    }

    if market_name in ou_routes:
        append_ou_market(ou_routes[market_name], nom, date_str, tournoi, link, y)
        return

    if market_name == "Vainqueur du match":
        append_two_way_moneyline(Win, nom, date_str, tournoi, link, selections)
        return
    if market_name == "Set 1 - Vainqueur":
        append_two_way_moneyline(Winset1, nom, date_str, tournoi, link, selections)
        return
    if market_name == "Nombre exact de sets":
        append_multi_way_first_two_as_moneyline(Totalset, nom, date_str, tournoi, link, y)
        return
    if market_name == "Ecart de sets":
        if len(selections) >= 2:
            cut1 = contains_specific_pattern_set(selections[0].get("name"))
            cut2 = contains_specific_pattern_set(selections[1].get("name"))
            odd1 = calculate_odd(selections[0])
            odd2 = calculate_odd(selections[1])
            if odd1 and odd2:
                HDPSet.loc[len(HDPSet)] = {
                    'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
                    'cut 1': cut1, 'cut 2': cut2, 'cote 1': odd1, 'cote 2': odd2,
                }
        return

    if market_name in JEUX_HANDICAP_FT_MARKET_NAMES and len(selections) >= 2:
        cut1 = contains_specific_pattern_games(selections[0].get("name"))
        cut2 = contains_specific_pattern_games(selections[1].get("name"))
        odd1 = calculate_odd(selections[0])
        odd2 = calculate_odd(selections[1])
        if odd1 and odd2:
            HDJeuxFT.loc[len(HDJeuxFT)] = {
                'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
                'cut 1': cut1, 'cut 2': cut2, 'cote 1': odd1, 'cote 2': odd2,
            }
        return
    if market_name in JEUX_HANDICAP_SET1_MARKET_NAMES and len(selections) >= 2:
        cut1 = contains_specific_pattern_games(selections[0].get("name"))
        cut2 = contains_specific_pattern_games(selections[1].get("name"))
        odd1 = calculate_odd(selections[0])
        odd2 = calculate_odd(selections[1])
        if odd1 and odd2:
            HDJeuxSet1.loc[len(HDJeuxSet1)] = {
                'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
                'cut 1': cut1, 'cut 2': cut2, 'cote 1': odd1, 'cote 2': odd2,
            }


async def run_scrape():
    """Collecte Unibet tennis ; remplit les DataFrames (utilisé seul ou via unibet_all_json)."""
    reset_dataframes()
    TM = []

    url = f"https://www.unibet.fr/zones/v3/sportnode/markets.json?nodeId={UNIBET_SPORT_NODE_ID}&filter=Top%2520Paris&marketname=Vainqueur%2520du%2520match"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.unibet.fr/',
        'Origin': 'https://www.unibet.fr'
    }
    connector = aiohttp.TCPConnector(limit=64, limit_per_host=24, ttl_dns_cache=600)
    async with aiohttp.ClientSession(
        headers=headers, connector=connector, timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
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
        if not marketsByType or len(marketsByType) == 0:
            print("Aucun marché trouvé")
            return

        days = marketsByType[0].get("days")
        if not days:
            print("Aucun jour trouvé")
            return

        for day in days:
            events = day.get("events", [])
            for event in events:
                tournoi = event.get("competitionName")
                date = event.get("eventStartDate")
                title = event.get("eventName")
                event_id = event.get("eventId")
                markets = event.get("markets", [])
                if markets and len(markets) > 0:
                    link = "unibet.fr" + markets[0].get("eventFriendlyUrl", "")
                    TM.append([event_id, title, tournoi, date, link])

        if not TM:
            print("Aucun événement trouvé")
            return

        semaphore = asyncio.Semaphore(14)

        async def fetch_with_semaphore(event_id):
            async with semaphore:
                return await fetch_event_data(session, event_id)

        tasks = [fetch_with_semaphore(event_id) for event_id, _, _, _, _ in TM]
        event_data_list = await asyncio.gather(*tasks)

        for event_data, (event_id, nom, tournoi, date, link) in zip(event_data_list, TM):
            if event_data is None:
                continue
            link = link_from_event_payload(event_data, link)

            date_str = datage(date)
            if date_str is None:
                continue

            marketClassList = event_data.get("marketClassList", [])

            for x in marketClassList:
                marketList = x.get("marketList", [])
                for y in marketList:
                    selections = y.get("selections", [])
                    if not selections:
                        continue
                    process_market_row(nom, date_str, tournoi, link, y)


async def main():
    await run_scrape()

    print("\n=== Résultats de la collecte (spec tennis PDF) ===")
    for label, df in [
        ("OUJEUSet1", OUJEUSet1),
        ("OUJEU (total jeux match)", OUJEU),
        ("OUJoueur1 (total jeux J1 match)", OUJoueur1),
        ("OUJoueur2 (total jeux J2 match)", OUJoueur2),
        ("OUJoueur1Set1", OUJoueur1Set1),
        ("OUJoueur2Set1", OUJoueur2Set1),
        ("Win (moneyline match)", Win),
        ("Winset1 (moneyline 1er set)", Winset1),
        ("Totalset (nombre exact sets, 2 1res cotes)", Totalset),
        ("HDPSet (handicap sets)", HDPSet),
        ("HDJeuxFT (handicap jeux match, si dispo)", HDJeuxFT),
        ("HDJeuxSet1 (handicap jeux S1, si dispo)", HDJeuxSet1),
    ]:
        print(f"{label}: {len(df)} lignes")
    print(OUJEUSet1)
    print(OUJEU)
    print(OUJoueur1)
    print(OUJoueur2)
    print(OUJoueur1Set1)
    print(OUJoueur2Set1)
    print(Win)
    print(Winset1)
    print(Totalset)
    print(HDPSet)
    print(HDJeuxFT)
    print(HDJeuxSet1)


if __name__ == "__main__":
    asyncio.run(main())

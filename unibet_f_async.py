"""
Scraping prématch football Unibet.fr — aligné sur scraping_specifications_clean.pdf (§ Football).

Couvert (si le marché existe sur Unibet, sinon ignoré) — réf. scraping_specifications_clean.pdf § Football :
  Moneyline mi-temps / match, double chance HT/FT, DNB mi-temps, DNB match,
  handicaps buts (3 issues), totaux buts match / 1ère MT / 2e MT,
  totaux buts équipe dom/ext (FT, 1ère MT, 2e MT), BTTS match / 1ère MT / 2e MT,
  1er but match / 1ère MT, victoire sans encaisser (FT).
  (Équipe « marque Oui/Non » type Pinnacle : pas d’équivalent simple listé sur Unibet.fr → ignoré.)

Les libellés exacts Unibet.fr sont mappés explicitement ci-dessous.
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

# --- Moneyline & double chance (3 issues) ---
Win = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
WinHT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
C2 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', '1N', '12', 'N2'])
C2HT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', '1N', '12', 'N2'])

# Draw no bet (2 issues)
WinHT_DNB = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cote 1', 'cote 2'])
DNB_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cote 1', 'cote 2'])

# Totaux O/U (over = Plus, under = Moins)
OUMatch = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUHT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OU2H = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUTeamHome_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUTeamAway_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUTeamHome_HT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUTeamAway_HT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUTeamHome_2H = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])
OUTeamAway_2H = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'over', 'under'])

# BTTS & 1er but (FT: 3 issues avec « Aucun » au centre si applicable — ordre API)
BTTS_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'oui', 'non'])
BTTS_HT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'oui', 'non'])
BTTS_2H = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'oui', 'non'])
FirstGoal_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
FirstGoal_HT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])

# Victoire sans encaisser (oui/non)
WinToNil_Home_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'oui', 'non'])
WinToNil_Away_FT = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'oui', 'non'])

# Handicaps buts — 3 issues par ligne (Unibet)
HDP_FT_3 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
HDP_HT_3 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])
HDP_2H_3 = pd.DataFrame(columns=['nom du match', 'date du match', 'tournoi', 'lien', 'cut', 'cote 1', 'cote N', 'cote 2'])

UNIBET_SPORT_NODE_ID = 703696073


def get_export_tables():
    return [
        ("Win", Win),
        ("WinHT", WinHT),
        ("C2", C2),
        ("C2HT", C2HT),
        ("WinHT_DNB", WinHT_DNB),
        ("DNB_FT", DNB_FT),
        ("OUMatch", OUMatch),
        ("OUHT", OUHT),
        ("OU2H", OU2H),
        ("OUTeamHome_FT", OUTeamHome_FT),
        ("OUTeamAway_FT", OUTeamAway_FT),
        ("OUTeamHome_HT", OUTeamHome_HT),
        ("OUTeamAway_HT", OUTeamAway_HT),
        ("OUTeamHome_2H", OUTeamHome_2H),
        ("OUTeamAway_2H", OUTeamAway_2H),
        ("BTTS_FT", BTTS_FT),
        ("BTTS_HT", BTTS_HT),
        ("BTTS_2H", BTTS_2H),
        ("FirstGoal_FT", FirstGoal_FT),
        ("FirstGoal_HT", FirstGoal_HT),
        ("WinToNil_Home_FT", WinToNil_Home_FT),
        ("WinToNil_Away_FT", WinToNil_Away_FT),
        ("HDP_FT_3", HDP_FT_3),
        ("HDP_HT_3", HDP_HT_3),
        ("HDP_2H_3", HDP_2H_3),
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


def append_three_way(df, nom, date_str, tournoi, link, y):
    selections = y.get("selections") or []
    if len(selections) < 3:
        return
    o1, o2, o3 = calculate_odd(selections[0]), calculate_odd(selections[1]), calculate_odd(selections[2])
    if o1 and o2 and o3:
        mt = y.get("marketType") or ""
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cut': mt, 'cote 1': o1, 'cote N': o2, 'cote 2': o3,
        }


def append_double_chance(df, nom, date_str, tournoi, link, y):
    selections = y.get("selections") or []
    if len(selections) < 3:
        return
    o1, o2, o3 = calculate_odd(selections[0]), calculate_odd(selections[1]), calculate_odd(selections[2])
    if o1 and o2 and o3:
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            '1N': o1, '12': o2, 'N2': o3,
        }


def append_two_way(df, nom, date_str, tournoi, link, y):
    selections = y.get("selections") or []
    if len(selections) < 2:
        return
    o1, o2 = calculate_odd(selections[0]), calculate_odd(selections[1])
    if o1 and o2:
        df.loc[len(df)] = {
            'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
            'cote 1': o1, 'cote 2': o2,
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
        'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
        'cut': y.get("marketType"), 'oui': oui, 'non': non,
    }


def append_handicap_three_way(df, nom, date_str, tournoi, link, y):
    """Handicap football Unibet : 3 sélections par ligne."""
    selections = y.get("selections") or []
    if len(selections) < 3:
        return
    o1, o2, o3 = calculate_odd(selections[0]), calculate_odd(selections[1]), calculate_odd(selections[2])
    if not (o1 and o2 and o3):
        return
    mn = y.get("marketName") or ""
    mt = y.get("marketType") or ""
    cut = f"{mn} — {mt}"
    df.loc[len(df)] = {
        'nom du match': nom, 'date du match': date_str, 'tournoi': tournoi, 'lien': link,
        'cut': cut, 'cote 1': o1, 'cote N': o2, 'cote 2': o3,
    }


def process_market(nom, date_str, tournoi, link, y):
    mn = y.get("marketName")
    if not mn:
        return

    if mn == "Résultat du match":
        append_three_way(Win, nom, date_str, tournoi, link, y)
        return
    if mn == "1ère Mi-temps - Résultat":
        append_three_way(WinHT, nom, date_str, tournoi, link, y)
        return
    if mn in ("Double chance", "Chance double"):
        append_double_chance(C2, nom, date_str, tournoi, link, y)
        return
    if mn in ("1ère Mi-temps - Double chance", "1ère Mi-temps - Chance double"):
        append_double_chance(C2HT, nom, date_str, tournoi, link, y)
        return
    if mn == "1ère Mi-temps - Résultat hors match nul":
        append_two_way(WinHT_DNB, nom, date_str, tournoi, link, y)
        return
    if mn == "Résultat du match (remboursé si match nul)":
        append_two_way(DNB_FT, nom, date_str, tournoi, link, y)
        return

    ou_map = {
        "Total de buts": OUMatch,
        "1ère Mi-temps - Total de buts": OUHT,
        "2e Mi-temps - Total de buts": OU2H,
        "Total de buts - Equipe à domicile": OUTeamHome_FT,
        "Total de buts - Equipe à l'extérieur": OUTeamAway_FT,
        "1ère Mi-temps - Total de buts - Equipe à domicile": OUTeamHome_HT,
        "1ère Mi-temps - Total de buts - Equipe à l'extérieur": OUTeamAway_HT,
        "2e Mi-temps - Total de buts - Equipe à domicile": OUTeamHome_2H,
        "2e Mi-temps - Total de buts - Equipe à l'extérieur": OUTeamAway_2H,
    }
    if mn in ou_map:
        append_ou_market(ou_map[mn], nom, date_str, tournoi, link, y)
        return

    if mn == "But pour les 2 équipes":
        append_yes_no(BTTS_FT, nom, date_str, tournoi, link, y)
        return
    if mn == "1ère Mi-temps - Les 2 équipes marquent":
        append_yes_no(BTTS_HT, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Mi-temps - Les 2 équipes marquent":
        append_yes_no(BTTS_2H, nom, date_str, tournoi, link, y)
        return

    if mn == "Qui marquera le 1er but":
        append_three_way(FirstGoal_FT, nom, date_str, tournoi, link, y)
        return
    if mn == "1ère Mi-temps - Qui marquera le 1er but":
        append_three_way(FirstGoal_HT, nom, date_str, tournoi, link, y)
        return

    if mn == "L'équipe à domicile gagne sans prendre de but":
        append_yes_no(WinToNil_Home_FT, nom, date_str, tournoi, link, y)
        return
    if mn == "L'équipe à l'extérieur gagne sans prendre de but":
        append_yes_no(WinToNil_Away_FT, nom, date_str, tournoi, link, y)
        return

    if mn in ("Ecart entre équipes", "Ecart entre les équipes (3 options)"):
        append_handicap_three_way(HDP_FT_3, nom, date_str, tournoi, link, y)
        return
    if mn == "1ère Mi-temps - Ecart entre les équipes":
        append_handicap_three_way(HDP_HT_3, nom, date_str, tournoi, link, y)
        return
    if mn == "2e Mi-temps - Ecart entre les équipes":
        append_handicap_three_way(HDP_2H_3, nom, date_str, tournoi, link, y)
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
    url = f"https://www.unibet.fr/zones/v3/sportnode/markets.json?nodeId={UNIBET_SPORT_NODE_ID}&filter=R%25C3%25A9sultat&marketname=R%25C3%25A9sultat%2520du%2520match"

    async with unibet_client_session() as session:
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
        ("Win (moneyline match)", Win),
        ("WinHT (moneyline 1ère MT)", WinHT),
        ("C2 (double chance match)", C2),
        ("C2HT (double chance 1ère MT)", C2HT),
        ("WinHT_DNB (1ère MT hors nul)", WinHT_DNB),
        ("DNB_FT (match remb. si nul)", DNB_FT),
        ("OUMatch (total buts match)", OUMatch),
        ("OUHT (total buts 1ère MT)", OUHT),
        ("OU2H (total buts 2e MT)", OU2H),
        ("OUTeamHome_FT", OUTeamHome_FT),
        ("OUTeamAway_FT", OUTeamAway_FT),
        ("OUTeamHome_HT", OUTeamHome_HT),
        ("OUTeamAway_HT", OUTeamAway_HT),
        ("OUTeamHome_2H", OUTeamHome_2H),
        ("OUTeamAway_2H", OUTeamAway_2H),
        ("BTTS_FT", BTTS_FT),
        ("BTTS_HT", BTTS_HT),
        ("BTTS_2H", BTTS_2H),
        ("FirstGoal_FT", FirstGoal_FT),
        ("FirstGoal_HT", FirstGoal_HT),
        ("WinToNil_Home_FT", WinToNil_Home_FT),
        ("WinToNil_Away_FT", WinToNil_Away_FT),
        ("HDP_FT_3 (handicap buts match)", HDP_FT_3),
        ("HDP_HT_3 (handicap buts 1ère MT)", HDP_HT_3),
        ("HDP_2H_3 (handicap buts 2e MT)", HDP_2H_3),
    ]
    print("\n=== Résultats foot (spec PDF) ===")
    for label, df in frames:
        print(f"{label}: {len(df)} lignes")
    for _, df in frames:
        print(df)


if __name__ == "__main__":
    asyncio.run(main())

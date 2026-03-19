# unibet-fr-scraper

Scrape **prématch** [Unibet.fr](https://www.unibet.fr) (tennis, foot, basket, hockey) → un JSON type `output.json` (`generated_at`, `sports`, `competitions`, `markets`, `props`, `url` par match).

## En local

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python unibet_all_json.py              # écrit output.json à la racine
python unibet_all_json.py --pretty   # JSON indenté
```

**Tor (optionnel)** — avec le daemon `tor` qui écoute en local (souvent `127.0.0.1:9050`) :

```bash
UNIBET_USE_TOR=1 python unibet_all_json.py
# ou TOR_SOCKS_PROXY=socks5h://127.0.0.1:9150 … si tu utilises le port du Tor Browser
```

**NordVPN sur VPS** — deux cas :

1. **VPN au niveau du système** (`nordvpn connect`, WireGuard, etc.) : tout le trafic du serveur sort déjà par NordVPN → lance le script **sans** variable ; si ça ne sort pas par le VPN, vérifie le **split tunneling**, ou que le script ne tourne pas dans un **conteneur** avec son propre réseau (Docker : `network_mode: host` sur l’hôte où NordVPN est actif, ou route manuelle).
2. **Proxy SOCKS NordVPN (recommandé si le scraper est isolé réseau)** : dans l’espace client NordVPN, active les identifiants **SOCKS5** / serveur proxy. Puis soit une URL complète, soit des variables séparées (mot de passe avec caractères spéciaux pris en charge) :

```bash
export UNIBET_SOCKS_PROXY='socks5h://UTILISATEUR_SOCKS:MOTDEPASSE@exemple.socks.nordhold.net:1080'
python unibet_all_json.py
```

ou

```bash
export NORDVPN_SOCKS_HOST='exemple.socks.nordhold.net'
export NORDVPN_SOCKS_USER='…'
export NORDVPN_SOCKS_PASS='…'
# NORDVPN_SOCKS_PORT=1080   # défaut
python unibet_all_json.py
```

Avec SOCKS applicatif, **`HTTPS_PROXY` ne doit pas être défini** en même temps (sinon risque de double proxy). Utilise le hostname SOCKS indiqué par NordVPN pour le pays voulu.

## Railway

1. [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub** → choisir **ce repo**.
2. Pas de *root directory* à configurer : le `Dockerfile` est à la racine.
3. Variables optionnelles :
   - `OUTPUT_PATH` (défaut `/app/output.json`)
   - `SCRAPE_INTERVAL_SECONDS` : `0` = un run puis arrêt ; `3600` = scrape toutes les heures en boucle.
   - **`HTTPS_PROXY` / `HTTP_PROXY`** : si tu vois des **HTTP 403** sur `zones/v3/sportnode/markets.json`, c’est souvent le **WAF / anti-bot** qui bloque les IP de datacenter (Railway, AWS, etc.). Le client HTTP utilise `trust_env=True` : défini un proxy **résidentiel ou mobile** (FR de préférence), par ex. `HTTPS_PROXY=http://user:pass@host:port`.
   - **Tor via SOCKS** : `UNIBET_USE_TOR=1` + `aiohttp-socks`. Défaut : `TOR_SOCKS_PROXY=socks5h://127.0.0.1:9050`. **Ne pas** combiner avec `HTTPS_PROXY` si tu veux uniquement Tor (`trust_env` désactivé).
   - **SOCKS NordVPN / autre** : `UNIBET_SOCKS_PROXY=socks5h://…` ou `NORDVPN_SOCKS_HOST` + `NORDVPN_SOCKS_USER` + `NORDVPN_SOCKS_PASS` (voir section locale ci‑dessus). Même règle : pas de `HTTPS_PROXY` simultané.
   - **Limite Tor** : sorties Tor souvent bloquées par les bookmakers ; **limite NordVPN** : IP datacenter possible selon offre — pas de garantie contre les 403.

Avant la première requête API, une visite de `https://www.unibet.fr/` récupère les cookies usuel du site ; sans proxy, le blocage peut malgré tout persister depuis le cloud.

Le fichier JSON est produit **dans le conteneur** ; pour l’exploiter ailleurs, ajoute upload (S3, webhook, etc.) selon ton flux.

## Licence / usage

Usage responsable, respect des CGU Unibet et du cadre légal du pays.

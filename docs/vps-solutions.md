# Faire tourner le scraper quand le VPS est bloqué (403, WAF)

## 1. Playwright / Chromium (même principe que le PDF Unibet V120)

Les requêtes passent par un **vrai navigateur** : ouverture de `https://www.unibet.fr/sport`, puis `fetch()` en JavaScript (cookies + TLS Chromium). Souvent suffisant là où **aiohtml** est refusé sur une IP datacenter.

```bash
pip install -r requirements.txt
python -m playwright install chromium   # une fois
UNIBET_USE_PLAYWRIGHT=1 python unibet_all_json.py
```

Variables utiles :

| Variable | Défaut | Rôle |
|----------|--------|------|
| `UNIBET_PLAYWRIGHT_HEADLESS` | `1` | `0` pour déboguer avec fenêtre |
| `UNIBET_PLAYWRIGHT_WARMUP_SLEEP` | `2` | Pause après chargement de /sport |
| `UNIBET_PLAYWRIGHT_CONCURRENCY` | `10` | Pages parallèles pour `event.json` |
| `UNIBET_PLAYWRIGHT_PROXY` | — | Proxy HTTP pour Chromium, ex. `http://host:port` (optionnel) |

**Docker / CI** : l’image installe Chromium (voir `Dockerfile`). Sur un VPS nu, installe les deps système comme pour Playwright ou utilise l’image construite à partir du repo.

**Limite** : si Unibet bloque **l’IP** du VPS sans regarder le client, il faudra encore un **VPN**, **proxy résidentiel**, ou faire tourner le job derrière une **box** résidentielle.

---

## 2. Proxy résidentiel / mobile (`UNIBET_PROXY`)

```bash
export UNIBET_PROXY='http://USER:PASS@gateway.fournisseur:PORT'
python unibet_all_json.py
```

---

## 3. SOCKS (`UNIBET_SOCKS_PROXY` ou Tor)

Voir README (NordVPN, Tor, etc.).

---

## 4. VPN sur tout le VPS

[docs/vps-nordvpn-openvpn.md](vps-nordvpn-openvpn.md) — une fois le tunnel actif, pas besoin de Playwright *pour l’egress*, mais tu peux toujours combiner VPN + aiohttp classique (sans `UNIBET_USE_PLAYWRIGHT`).

---

## 5. Scraper à la maison + envoi du JSON vers le VPS

`scripts/sync-output-to-vps.example.sh` (à adapter).

Respecte la loi et les CGU Unibet.

# unibet.fr

Dépôt minimal : `unibet_prematch_odds.py`.

## Distant GitHub

```text
https://github.com/lschaeffer57/unibet.fr.git
```

SSH :

```text
git@github.com:lschaeffer57/unibet.fr.git
```

## Première connexion (machine locale)

Si le dépôt n’existe pas encore sur GitHub : crée-le sur [github.com/new](https://github.com/new) avec le nom `unibet.fr` (sans initialiser README/License si tu pousses un dépôt existant).

Puis :

```bash
cd /chemin/vers/unibet.fr
git remote set-url origin https://github.com/lschaeffer57/unibet.fr.git
git push -u origin main
```

Avec authentification HTTPS, un [Personal Access Token](https://github.com/settings/tokens) remplace le mot de passe. Avec SSH, ajoute ta clé dans les paramètres GitHub.

## Railway (logs & cycles)

Le scraper écrit sur **stdout** en UTC, une ligne par étape : `phase=session|token|listing|details|build_json|summary`, puis `event=cycle_end` avec `wall_ms` (durée totale du cycle).

Variables utiles dans le service Railway :

| Variable | Rôle |
|----------|------|
| `SCRAPER_LOOP_SECONDS` | Intervalle entre deux runs complets (ex. `300`). `0` = un seul run puis arrêt. |
| `OUTPUT_PATH` | Fichier JSON écrit (défaut `/tmp/output_prematch.json`). |
| `LOG_LEVEL` | `INFO` (défaut), `DEBUG`, `WARNING`… |

Démarrage : `start.sh` (voir `railway.toml`) avec `PYTHONUNBUFFERED=1` pour des logs en temps réel dans l’onglet *Deployments → Logs*.

"""En-têtes + warm-up session Unibet (réduit les 403 datacenter ; proxy HTTP ou Tor via env)."""

from __future__ import annotations

import os

import aiohttp
from aiohttp.connector import BaseConnector

# Même “empreinte” navigateur que des requêtes XHR depuis unibet.fr
UNIBET_REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.unibet.fr/",
    "Origin": "https://www.unibet.fr",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "DNT": "1",
}


async def warm_unibet_session(session: aiohttp.ClientSession) -> None:
    """Page d’accueil pour cookies / jetons souvent exigés avant l’API zones/."""
    h = {
        **UNIBET_REQUEST_HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        async with session.get(
            "https://www.unibet.fr/",
            headers=h,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=25),
        ) as resp:
            await resp.read()
    except Exception:
        pass


def use_tor() -> bool:
    v = os.environ.get("UNIBET_USE_TOR", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def tor_socks_url() -> str:
    """SOCKS5 distant (résolution DNS côté proxy) — défaut port Tor."""
    return os.environ.get("TOR_SOCKS_PROXY", "socks5h://127.0.0.1:9050").strip()


def unibet_trust_env() -> bool:
    """Sous Tor, désactivé pour éviter de chaîner HTTPS_PROXY en plus du SOCKS."""
    return not use_tor()


def unibet_connector() -> BaseConnector:
    if use_tor():
        try:
            from aiohttp_socks import ProxyConnector
        except ImportError as e:
            raise ImportError(
                "UNIBET_USE_TOR activé : installe aiohttp-socks (déjà dans requirements.txt)."
            ) from e
        return ProxyConnector.from_url(
            tor_socks_url(),
            limit=64,
            limit_per_host=24,
            ttl_dns_cache=600,
        )
    return aiohttp.TCPConnector(limit=64, limit_per_host=24, ttl_dns_cache=600)

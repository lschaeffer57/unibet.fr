"""Lien fiche match depuis event.json (eventHeader.friendlyUrl)."""

from __future__ import annotations


def link_from_event_payload(event_data: dict | None, fallback_link: str) -> str:
    if not event_data:
        return fallback_link
    fu = (event_data.get("eventHeader") or {}).get("friendlyUrl")
    if not (isinstance(fu, str) and fu.strip()):
        return fallback_link
    p = fu.strip()
    if p.startswith("/"):
        return "unibet.fr" + p
    if p.lower().startswith("unibet.fr"):
        return p
    return "unibet.fr/" + p.lstrip("/")

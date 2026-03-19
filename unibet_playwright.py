"""Chromium + Playwright : fetch same-origin depuis la page (comme le script PDF Unibet V120). Utile sur VPS quand aiohttp se fait bloquer."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiohttp

from unibet_http import (
    UNIBET_REQUEST_HEADERS,
    unibet_connector,
    unibet_trust_env,
    warm_unibet_session,
)


def use_playwright() -> bool:
    v = os.environ.get("UNIBET_USE_PLAYWRIGHT", "").strip().lower()
    return v in ("1", "true", "yes", "on")


class PlaywrightFetcher:
    """Contexte navigateur réchauffé sur /sport puis fetch(url) en JS (cookies + TLS Chromium)."""

    def __init__(self) -> None:
        self._playwright = None
        self._browser = None
        self._context = None
        n = int(os.environ.get("UNIBET_PLAYWRIGHT_CONCURRENCY", "10").strip() or "10")
        self._semaphore = asyncio.Semaphore(max(1, min(n, 32)))

    async def __aenter__(self) -> PlaywrightFetcher:
        from playwright.async_api import async_playwright

        headless = os.environ.get("UNIBET_PLAYWRIGHT_HEADLESS", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        ua = os.environ.get(
            "UNIBET_PLAYWRIGHT_UA",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        proxy_url = os.environ.get("UNIBET_PLAYWRIGHT_PROXY", "").strip()
        launch_kwargs: dict = {
            "headless": headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if proxy_url:
            launch_kwargs["proxy"] = {"server": proxy_url}

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        self._context = await self._browser.new_context(
            user_agent=ua,
            locale="fr-FR",
            timezone_id="Europe/Paris",
        )
        timeout_ms = int(os.environ.get("UNIBET_PLAYWRIGHT_TIMEOUT_MS", "90000").strip() or "90000")
        self._context.set_default_timeout(timeout_ms)

        page = await self._context.new_page()
        try:
            await page.goto(
                "https://www.unibet.fr/sport",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.mouse.move(100, 100)
            await asyncio.sleep(
                float(os.environ.get("UNIBET_PLAYWRIGHT_WARMUP_SLEEP", "2").strip() or "2")
            )
        finally:
            await page.close()
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._context is not None:
            await self._context.close()
        if self._browser is not None:
            await self._browser.close()
        if self._playwright is not None:
            await self._playwright.stop()

    async def get_text(self, url: str) -> str | None:
        try:
            async with self._semaphore:
                page = await self._context.new_page()
                try:
                    result = await page.evaluate(
                        """async (url) => {
                            const r = await fetch(url, { credentials: 'include' });
                            const text = await r.text();
                            return { ok: r.ok, status: r.status, text: text };
                        }""",
                        url,
                    )
                    if not result.get("ok"):
                        st = result.get("status")
                        print(f"Erreur HTTP {st} lors de la récupération de {url}: Forbidden" if st == 403 else f"Erreur HTTP {st} lors de la récupération de {url}")
                        return None
                    return result.get("text")
                finally:
                    await page.close()
        except Exception as e:
            print(f"Erreur lors de la récupération de {url}: {e}")
            return None


@asynccontextmanager
async def unibet_client_session() -> AsyncIterator[aiohttp.ClientSession | PlaywrightFetcher]:
    """Session aiohttp (défaut) ou fetcher Playwright si UNIBET_USE_PLAYWRIGHT=1."""
    if use_playwright():
        async with PlaywrightFetcher() as pw:
            yield pw
        return
    connector = unibet_connector()
    async with aiohttp.ClientSession(
        headers=UNIBET_REQUEST_HEADERS,
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=30),
        trust_env=unibet_trust_env(),
    ) as session:
        await warm_unibet_session(session)
        yield session

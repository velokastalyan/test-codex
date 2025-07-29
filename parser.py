#!/usr/bin/env python
# -*- coding: utf‑8 -*-

"""
Парсер каталога https://sprint‑rowery.pl/rowery
Собирает: category, title, price, link
Сохраняет в sprint_rowery_<stamp>.csv + sprint_rowery_<stamp>.xlsx

Запуск:

    python parser.py

Остановить в любой момент: Ctrl+C — скрипт сохранит то, что успел собрать.
"""

import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ─── настройки ────────────────────────────────────────────────────────────────
BASE_URL   = "https://sprint-rowery.pl/rowery?product_list_limit=60"
HEADERS    = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 64          # потоков на карточки
TIMEOUT     = 20
# -----------------------------------------------------------------------------

# «ручной» список типов — подстраховка, если не удаётся вытащить категорию из
# JSON/хлебных крошек. Берём первое совпадение по префиксу (регистр игнорируем)
TYPE_PREFIXES = [
    "Rower górski", "Rower szosowy", "Rower gravel", "Rower crossowy",
    "Rower fitness", "Rower elektryczny", "Rower trekkingowy",
    "Rower dziecięcy", "Rower BMX", "Rower enduro", "Rower downhill"
]

def get_soup(url: str) -> BeautifulSoup:
    """Запрашиваем страницу и возвращаем объект BeautifulSoup."""
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


# ───── данные из плитки ───────────────────────────────────────────────────────
def parse_tile(tile, page_url: str) -> dict:
    title_el = tile.select_one("h3.product-item__name_heading, a.product-item-link")
    price_el = tile.select_one("div.product-price--final-price, span.price")
    link_el  = tile.select_one("a.product-item-link")

    title = title_el.get_text(" ", strip=True) if title_el else ""
    price = price_el.get_text(" ", strip=True) if price_el else ""
    link  = urljoin(page_url, link_el["href"]) if link_el and link_el.has_attr("href") else ""

    return {
        "title": title,
        "price": price,
        "link":  link,
        "category": ""   # заполним позже
    }


# ───── категория на странице товара ───────────────────────────────────────────
def fetch_category(url: str) -> str:
    """Возвращает строку с категорией или ""."""
    if not url:
        return ""

    try:
        soup = get_soup(url)
    except Exception:
        return ""

    # ① Пробуем structured‑data (BreadcrumbList)
    try:
        for script in soup.find_all("script", type="application/ld+json"):
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
                crumbs = [it["item"]["name"] for it in data.get("itemListElement", [])]
                if len(crumbs) >= 3:          # [Strona główna, …категория…, Название товара]
                    return crumbs[-2]         # предпоследний элемент — категория
    except Exception:
        pass                                  # переходим к другим методам

    # ② Пробуем обычные HTML‑крошки
    crumbs = soup.select("ol.breadcrumbs li a")
    if len(crumbs) >= 3:
        return crumbs[-2].get_text(strip=True)

    # ③ Фоллбэк: берём тип из TITLE (по префиксам)
    page_title = soup.title.get_text(" ", strip=True) if soup.title else ""
    for prefix in TYPE_PREFIXES:
        if page_title.lower().startswith(prefix.lower()):
            return prefix

    return ""


# ───── парс одной страницы каталога ───────────────────────────────────────────
def parse_page(url: str) -> tuple[list[dict], str | None]:
    print(f"[+] обрабатываем: {url}", end=" ")
    soup = get_soup(url)

    tiles = soup.select("div.product-item-info")
    print(f"| карточек на странице: {len(tiles)}")

    items = [parse_tile(t, url) for t in tiles]

    # подтягиваем категории параллельно
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut2idx = {pool.submit(fetch_category, it["link"]): i
                   for i, it in enumerate(items) if it["link"]}
        for fut in as_completed(fut2idx):
            items[fut2idx[fut]]["category"] = fut.result() or ""

    nxt = soup.select_one("li.pages-item-next > a, a.action.next")
    next_url = urljoin(url, nxt["href"]) if nxt and nxt.has_attr("href") else None
    return items, next_url


# ───── полный обход каталога ──────────────────────────────────────────────────
def crawl(start_url: str) -> list[dict]:
    all_items, url = [], start_url
    while url:
        page_items, url = parse_page(url)
        all_items.extend(page_items)
    return all_items


# ───── сохранение ─────────────────────────────────────────────────────────────
def save(data: list[dict]):
    df = pd.DataFrame(data)
    stamp = datetime.now().strftime("%d. %m. %Y %H-%M")
    df.to_csv(f"sprint_rowery_{stamp}.csv",  sep=";", index=False, encoding="utf-8-sig")
    df.to_excel(f"sprint_rowery_{stamp}.xlsx", index=False)


# ───── точка входа ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t0 = time.time()
    collected: list[dict] = []

    try:
        collected = crawl(BASE_URL)
    except KeyboardInterrupt:
        print("\n\u26A0\ufe0f  Остановлено вручную — сохраню то, что успел собрать…")
    finally:
        if collected:
            save(collected)
            print(f"\n\u2705  Сохранено {len(collected)} товаров.")
        else:
            print("\n\u26A0\ufe0f  Нечего сохранять — список пуст.")

        print(f"\n\u23F1  Время работы: {time.time() - t0:.1f} сек.")

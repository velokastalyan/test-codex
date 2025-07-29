#!/usr/bin/env python
# -*- coding: utf‑8 -*-

"""
Парсер каталога https://sprint-rowery.pl/rowery
Собирает: category, title, price, link
Сохраняет в sprint_rowery.csv + sprint_rowery.xlsx

Запуск:
    python parser.py

Остановить в любой момент: Ctrl+C — скрипт сохранит то, что уже собрано.
"""

import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ─── настройки ────────────────────────────────────────────────────────────────
BASE_URL   = "https://sprint-rowery.pl/rowery?product_list_limit=60"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 64      # потоков на карточки
TIMEOUT     = 20
# ───────────────────────────────────────────────────────────────────────────────


def get_soup(url: str) -> BeautifulSoup:
    """Запрашиваем страницу и возвращаем объект BeautifulSoup."""
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


# ─── данные из плитки ─────────────────────────────────────────────────────────
def parse_tile(tile, page_url: str) -> dict:
    title_el = tile.select_one("h3.product-item__name_heading, a.product-item-link")
    price_el = tile.select_one("div.product-price-final-price, span.price")
    link_el  = tile.select_one("a.product-item-link")

    return {
        "title": title_el.get_text(" ", strip=True) if title_el else "",
        "price": price_el.get_text(" ", strip=True) if price_el else "",
        "link":  urljoin(page_url, link_el["href"]) if link_el and link_el.has_attr("href") else "",
    }


# ─── категория на странице товара ─────────────────────────────────────────────
def fetch_category(url: str) -> str:
    """Пытаемся вынуть категорию из HTML и/или JSON‑LD Breadcrumbs."""
    if not url:
        return ""

    soup = get_soup(url)

    # 1️⃣ классический <ol>/<ul> breadcrumbs
    crumbs = soup.select("ol.breadcrumbs li a, ul.breadcrumbs li a")
    if crumbs:
        return " > ".join(c.get_text(strip=True) for c in crumbs[1:-1])

    # 2️⃣ JSON‑LD BreadcrumbList
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            names = [it["name"] for it in data.get("itemListElement", [])]
            if names:
                return " > ".join(names[1:-1])

    return ""  # ничего не нашли


# ─── парс одной страницы каталога ─────────────────────────────────────────────
def parse_page(url: str) -> tuple[list[dict], str | None]:
    print(f"↻  {url}")
    soup = get_soup(url)

    tiles = soup.select("div.product-item-info")
    print(f"   карточек на странице: {len(tiles)}")

    items = [parse_tile(t, url) for t in tiles]

    # подтягиваем категории параллельно
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut2idx = {pool.submit(fetch_category, it["link"]): i
                   for i, it in enumerate(items) if it["link"]}
        for fut in as_completed(fut2idx):
            items[fut2idx[fut]]["category"] = fut.result()

    nxt = soup.select_one("li.pages-item-next > a, a.action.next")
    next_url = urljoin(url, nxt["href"]) if nxt and nxt.has_attr("href") else None
    return items, next_url


# ─── полный обход каталога ────────────────────────────────────────────────────
def crawl(start_url: str) -> list[dict]:
    all_items, url = [], start_url
    while url:
        page_items, url = parse_page(url)
        all_items.extend(page_items)
    return all_items


# ─── сохранение ───────────────────────────────────────────────────────────────
def save(data: list[dict]):
    df = pd.DataFrame(data)
    df.to_csv("sprint_rowery.csv", sep=";", index=False, encoding="utf‑8‑sig")
    # Excel
    try:
        df.to_excel("sprint_rowery.xlsx", index=False)   # требует openpyxl
    except ModuleNotFoundError:
        print("⚠️  openpyxl не установлен — .xlsx не создан.")


# ─── точка входа ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t0 = time.time()
    collected: list[dict] = []

    try:
        collected = crawl(BASE_URL)
    except KeyboardInterrupt:
        print("\n⏹  Остановлено вручную — сохраню то, что успел собрать…")
    finally:
        if collected:
            save(collected)
            print(f"✅  Сохранено {len(collected)} товаров.")
        else:
            print("⚠️  Нечего сохранять — список пуст.")
        print(f"⏱  Время работы: {time.time() - t0:.1f} сек.")

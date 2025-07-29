#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Парсер каталога https://sprint-rowery.pl/rowery
Сохраняет:  title, price, link, category
Вывод: sprint_rowery_<stamp>.csv и sprint_rowery_<stamp>.xlsx

Запуск:
    python parser.py
Прервать в любой момент: Ctrl+C — сохранит уже собранные данные.
"""

from __future__ import annotations

import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ───────── настройки ─────────
BASE_URL   = "https://sprint-rowery.pl/rowery?product_list_limit=60"
HEADERS    = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 64      # потоков на карточки
TIMEOUT     = 20
# ────────────────────────────


def get_soup(url: str) -> BeautifulSoup:
    """Запрашиваем страницу и возвращаем объект BeautifulSoup."""
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


# ───────── данные из плитки ─────────
def parse_tile(tile: BeautifulSoup, page_url: str) -> dict:
    title_el = tile.select_one("h3.product-item__name_heading, a.product-item-link")
    price_el = tile.select_one("div.product-price-final-price, span.price")
    link_el  = tile.select_one("a.product-item-link")

    return {
        "title": title_el.get_text(" ", strip=True) if title_el else "",
        "price": price_el.get_text(" ", strip=True) if price_el else "",
        "link" : urljoin(page_url, link_el["href"]) if link_el and link_el.has_attr("href") else "",
    }


# ───────── категория на странице товара ─────────
def fetch_category(url: str) -> str | None:
    """Возвращает категорию товара (последний пункт перед названием в крошках)."""
    if not url:
        return None

    try:
        soup = get_soup(url)
    except Exception:
        return None

    # крошки бывают <ol>, <ul> или <div class="breadcrumbs">
    crumbs = (
        soup.select("ol.breadcrumbs li a")
        or soup.select("ul.breadcrumbs li a")
        or soup.select("div.breadcrumbs a")
    )

    # стандартный случай – берём предпоследний элемент
    if len(crumbs) >= 2:
        return crumbs[-2].get_text(strip=True)

    # запасной вариант: ищем последнюю ссылку, содержащую «/rowery/»
    for a in reversed(crumbs):
        if "/rowery/" in a.get("href", ""):
            return a.get_text(strip=True)

    return None


# ───────── парс одной страницы каталога ─────────
def parse_page(url: str) -> tuple[list[dict], str | None]:
    print(f"[+] обрабатываем: {url}", end="  | ")
    soup = get_soup(url)

    tiles = soup.select("div.product-item-info")
    print(f"карточек на странице: {len(tiles)}")

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


# ───────── полный обход каталога ─────────
def crawl(start_url: str) -> list[dict]:
    all_items, url = [], start_url
    while url:
        page_items, url = parse_page(url)
        all_items.extend(page_items)
    return all_items


# ───────── сохранение ─────────
def save(data: list[dict]) -> None:
    df = pd.DataFrame(data)
    stamp = datetime.now().strftime("%d.%m.%Y %H-%M")
    csv_name  = f"sprint_rowery_{stamp}.csv"
    xlsx_name = f"sprint_rowery_{stamp}.xlsx"

    df.to_csv(csv_name,  sep=";", index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_name, index=False)
    print(f"✅  Сохранено {len(df)} товаров в {csv_name}/{xlsx_name}")


# ───────── точка входа ─────────
if __name__ == "__main__":
    t0 = time.time()
    collected: list[dict] = []

    try:
        collected = crawl(BASE_URL)
    except KeyboardInterrupt:
        print("\n⚠️ Остановлено вручную — сохраняю то, что успел собрать…")
    finally:
        if collected:
            save(collected)
        else:
            print("⚠️ Нечего сохранять — список пуст.")

        print(f"⏱️ Время работы: {time.time() - t0:.1f} сек.")

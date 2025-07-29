#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Парсер https://sprint-rowery.pl/rowery
Собирает title, price, link, category — только товары «в наличии».
Вывод: sprint_rowery_<dd.MM.yyyy HH-mm>.csv / .xlsx
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ───────── настройки ─────────
BASE_URL    = "https://sprint-rowery.pl/rowery?product_list_limit=60"
HEADERS     = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 64      # потоков на карточки
TIMEOUT     = 20
# ────────────────────────────


def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def parse_tile(tile: BeautifulSoup, page_url: str) -> dict:
    title_el = tile.select_one("h3.product-item__name_heading, a.product-item-link")
    price_el = tile.select_one("div.product-price-final-price, span.price")
    link_el  = tile.select_one("a.product-item-link")

    return {
        "title": title_el.get_text(" ", strip=True) if title_el else "",
        "price": price_el.get_text(" ", strip=True) if price_el else "",
        "link" : urljoin(page_url, link_el["href"]) if link_el and link_el.has_attr("href") else "",
    }


def fetch_category_availability(url: str) -> tuple[str | None, bool]:
    """Возвращает (category, in_stock) для товара."""
    if not url:
        return None, False

    try:
        soup = get_soup(url)
    except Exception:
        return None, False

    # ─── категория из крошек ───
    crumbs = (
        soup.select("ol.breadcrumbs li a")
        or soup.select("ul.breadcrumbs li a")
        or soup.select("div.breadcrumbs a")
    )
    category: str | None = None
    if len(crumbs) >= 2:
        category = crumbs[-2].get_text(strip=True)
    else:
        for a in reversed(crumbs):
            if "/rowery/" in a.get("href", ""):
                category = a.get_text(strip=True)
                break

    # ─── наличие из JSON‑LD ───
    in_stock = False
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        objects = data if isinstance(data, list) else [data]
        for obj in objects:
            if isinstance(obj, dict) and obj.get("@type") == "Product":
                offers = obj.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    availability = offers.get("availability", "")
                    if "InStock" in availability:
                        in_stock = True
                        break
        if in_stock:
            break

    return category, in_stock


def parse_page(url: str) -> tuple[list[dict], str | None]:
    print(f"[+] обрабатываем: {url}", end="  | ")
    soup = get_soup(url)

    tiles = soup.select("div.product-item-info")
    print(f"карточек на странице: {len(tiles)}")

    items = [parse_tile(t, url) for t in tiles]

    # подтягиваем категорию и наличие параллельно
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut2idx = {pool.submit(fetch_category_availability, it["link"]): i
                   for i, it in enumerate(items) if it["link"]}
        for fut in as_completed(fut2idx):
            cat, stock = fut.result()
            idx = fut2idx[fut]
            items[idx]["category"]  = cat or ""
            items[idx]["in_stock"]  = stock

    # фильтруем «в наличии»
    items_in_stock = [it for it in items if it.get("in_stock")]
    for it in items_in_stock:
        it.pop("in_stock", None)

    nxt = soup.select_one("li.pages-item-next > a, a.action.next")
    next_url = urljoin(url, nxt["href"]) if nxt and nxt.has_attr("href") else None
    return items_in_stock, next_url


def crawl(start_url: str) -> list[dict]:
    all_items, url = [], start_url
    while url:
        page_items, url = parse_page(url)
        all_items.extend(page_items)
    return all_items


def save(data: list[dict]) -> None:
    df = pd.DataFrame(data)
    stamp = datetime.now().strftime("%d.%m.%Y %H-%M")
    csv_name  = f"sprint_rowery_{stamp}.csv"
    xlsx_name = f"sprint_rowery_{stamp}.xlsx"

    df.to_csv(csv_name,  sep=";", index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_name, index=False)
    print(f"✅  Сохранено {len(df)} товаров в {csv_name}/{xlsx_name}")


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

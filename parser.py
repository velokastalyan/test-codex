#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Парсер https://sprint-rowery.pl/rowery
Сохраняет: category, title, price, link → sprint_rowery.{csv,xlsx}

Запуск: python parser.py
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ───── настройки ─────
BASE_URL = "https://sprint-rowery.pl/rowery?product_list_limit=60"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 64          # потоков на карточки
TIMEOUT = 20
# ─────────────────────


def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


# ---------- данные из плитки ----------
def parse_tile(tile, page_url: str) -> dict:
    title_el = tile.select_one("h3.product-item__name__heading, a.product-item-link")
    price_el = tile.select_one("div.product-price-final-price, span.price")
    link_el  = tile.select_one("a.product-item-link")

    return {
        "title": title_el.get_text(" ", strip=True) if title_el else "",
        "price": price_el.get_text(" ", strip=True) if price_el else "",
        "link":  urljoin(page_url, link_el["href"]) if link_el and link_el.has_attr("href") else "",
    }


# ---------- категория на странице товара ----------
def fetch_category(url: str) -> str:
    try:
        soup = get_soup(url)
    except Exception:
        return ""
    crumbs = soup.select("ol.breadcrumbs li a")
    return " > ".join(c.get_text(strip=True) for c in crumbs[1:-1]) if crumbs else ""


# ---------- парс одной страницы каталога ----------
def parse_page(url: str) -> tuple[list[dict], str | None]:
    print(f"→ {url}")
    soup = get_soup(url)

    items = [parse_tile(t, url) for t in soup.select("div.product-item-info")]

    # подтягиваем категории параллельно
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut2idx = {pool.submit(fetch_category, it["link"]): i for i, it in enumerate(items) if it["link"]}
        for fut in as_completed(fut2idx):
            items[fut2idx[fut]]["category"] = fut.result()

    # «дальше»
    nxt = soup.select_one("li.pages-item-next > a, a.action.next")
    next_url = urljoin(url, nxt["href"]) if nxt and nxt.has_attr("href") else None
    return items, next_url


# ---------- полный обход ----------
def crawl(start_url: str) -> list[dict]:
    all_items, url = [], start_url
    while url:
        page_items, url = parse_page(url)
        all_items.extend(page_items)
        # без sleep — нужная «пауза» создаётся самими запросами товаров
    return all_items


# ---------- save ----------
def save(data: list[dict]):
    df = pd.DataFrame(data)
    df.to_csv("sprint_rowery.csv", sep=";", index=False, encoding="utf-8-sig")
    df.to_excel("sprint_rowery.xlsx", index=False)
    print(f"\n✅ Готово. Сохранено: {len(df)} товаров.")


if __name__ == "__main__":
    t0 = time.time()
    dataset = crawl(BASE_URL)
    save(dataset)
    print(f"⏱ Время работы: {time.time()-t0:.1f} сек.")

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Парсер каталога https://sprint-rowery.pl/rowery
Собирает: название, цену, ссылку на товар, главное фото, описание, категорию
Сохраняет в sprint_rowery.{csv,xlsx}

Запуск:
    python parser.py
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ─────────────────────── настройки ────────────────────────
BASE_CATALOG_URL = "https://sprint-rowery.pl/rowery"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 8          # потоков для скачивания карточек
REQUEST_TIMEOUT = 20
# ────────────────────────────────────────────────────────────


def get_soup(url: str) -> BeautifulSoup:
    """Запрашиваем страницу и возвращаем объект BeautifulSoup."""
    res = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()
    return BeautifulSoup(res.text, "html.parser")


# ---------- «плитка» товара (название, цена, ссылка, мини‑фото) ----------
def parse_card(card, page_url: str) -> dict:
    title = (card.select_one("h3.product-item__name__heading") or
             card.select_one("a.product-item-link"))
    title = title.get_text(" ", strip=True) if title else ""

    price = card.select_one("div.product-price-final-price, span.price")
    price = price.get_text(" ", strip=True) if price else ""

    a_tag = card.select_one("a.product-item-link")
    link = urljoin(page_url, a_tag["href"]) if a_tag and a_tag.has_attr("href") else ""

    img_tag = card.select_one("img")
    thumb = img_tag["src"] if img_tag and img_tag.has_attr("src") else ""

    return {"title": title, "price": price, "link": link, "thumb": thumb}


# ---------- карточка товара (описание, категория, фото) ----------
def parse_details(url: str) -> dict:
    try:
        soup = get_soup(url)
    except Exception as e:
        print(f"[warn] не смог открыть {url}: {e}")
        return {"description": "", "category": "", "photo": ""}

    # 1) JSON‑LD (в нём сразу и фото, и описание)
    description = photo = ""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            description = BeautifulSoup(
                data.get("description", ""), "html.parser"
            ).get_text(" ", strip=True)
            photo = data.get("image", "")
            break

    # 2) категория из хлебных крошек
    crumbs = soup.select("ol.breadcrumbs li a")
    category = " > ".join(c.get_text(strip=True) for c in crumbs[1:-1]) if crumbs else ""

    return {"description": description, "category": category, "photo": photo}


# ---------- ссылка «Дальше» ----------
def get_next_url(soup: BeautifulSoup, current_url: str) -> str | None:
    link_tag = soup.select_one("li.pages-item-next > a, a.action.next")
    return urljoin(current_url, link_tag["href"]) if link_tag and link_tag.has_attr("href") else None


# ---------- обрабатываем одну страницу каталога ----------
def parse_page(url: str) -> tuple[list[dict], str | None]:
    print(f"→ {url}")
    soup = get_soup(url)

    # плитки товаров
    cards = soup.select("div.product-item-info")
    items = [parse_card(c, url) for c in cards]

    # дополняем подробностями, параллельно
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(parse_details, item["link"]): i for i, item in enumerate(items) if item["link"]}
        for fut in as_completed(futures):
            idx = futures[fut]
            items[idx].update(fut.result())

    # ссылка «дальше»
    next_url = get_next_url(soup, url)
    return items, next_url


# ---------- полный обход всех страниц ----------
def crawl_catalog(start_url: str) -> list[dict]:
    all_items, url = [], start_url
    while url:
        page_items, url = parse_page(url)
        all_items.extend(page_items)
        time.sleep(1)          # небольшая пауза, чтобы не спамить сайт
    return all_items


# ---------- сохранение ----------
def save(items: list[dict]) -> None:
    df = pd.DataFrame(items)
    df.to_csv("sprint_rowery.csv", sep=";", index=False, encoding="utf-8-sig")
    df.to_excel("sprint_rowery.xlsx", index=False)
    print(f"\n✅ Готово. Сохранено: {len(df)} товаров.")


if __name__ == "__main__":
    data = crawl_catalog(BASE_CATALOG_URL)
    save(data)

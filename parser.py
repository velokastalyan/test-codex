# -*- coding: utf-8 -*-
"""
Sprint-Rowery scraper with JS rendering + полная диагностика.
Требования (в активированном venv):
    pip install requests-html lxml lxml_html_clean pandas openpyxl
Запуск:
    python parser.py
Результат:
    raw_page.html, rendered_page.html, debug_page_*.html, debug_product_*.html
    output.csv, output.xlsx
"""

import os
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
from requests_html import HTMLSession

BASE_URL = "https://sprint-rowery.pl"
START_PATH = "/rowery"                 # корневая категория
RENDER_TIMEOUT = 30
RETRIES = 3
SLEEP = 1.0
SAVE_DEBUG = True
MAX_PAGES = 0                          # 0 = без лимита (если нужно ограничить, поставьте число)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "pl,en;q=0.9,ru;q=0.8",
}

@dataclass
class Product:
    title: str
    price: str
    link: str
    image: str
    category: str
    description: str


# ---------- утилиты ----------
def abs_url(href: str) -> str:
    return href if href.startswith("http") else urljoin(BASE_URL, href)

def norm(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def is_product_link(href: str) -> bool:
    if not href or href.startswith(("#", "mailto:", "tel:")):
        return False
    path = urlparse(href).path.lower()
    return any(k in path for k in ("/rower", "/produkt", "/product"))

def save_file(name: str, data: str):
    with open(name, "w", encoding="utf-8") as f:
        f.write(data)
    print(f"[SAVE] {os.path.abspath(name)}")

def session() -> HTMLSession:
    s = HTMLSession()
    s.headers.update(HEADERS)
    return s

def render(url: str):
    s = session()
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = s.get(url, timeout=30)
            # сохраняем сырой HTML первой страницы для диагностики
            if "page=1" in url or url.endswith(START_PATH) or url.endswith(START_PATH + "/"):
                save_file("raw_page.html", r.text)

            r.html.render(timeout=RENDER_TIMEOUT, sleep=1.0, reload=False, keep_page=True)
            html = r.html.html or ""
            if "page=1" in url or url.endswith(START_PATH) or url.endswith(START_PATH + "/"):
                save_file("rendered_page.html", html)

            if r.status_code == 200 and html:
                return r
        except Exception as e:
            last_err = e
            time.sleep(1.2 * attempt)
    print(f"[FAIL] render {url} -> {last_err}")
    return None


# ---------- парсинг листинга ----------
LIST_SELECTORS = [
    ".product-miniature",
    "article.product",
    "li.product",
    "div.product",
    "div.js-product",
    "[data-id-product]",
]

def parse_list(url: str, page_idx: int) -> Tuple[List[str], Optional[str]]:
    r = render(url)
    if not r:
        return [], None

    html = r.html.html or ""
    if SAVE_DEBUG:
        save_file(f"debug_page_{page_idx}.html", html)

    links: List[str] = []
    # 1) пробуем типовые селекторы карточек
    for sel in LIST_SELECTORS:
        cards = r.html.find(sel)
        if not cards:
            continue
        for c in cards:
            a = c.find("a[href]", first=True)
            if not a:
                continue
            href = abs_url(a.attrs.get("href", ""))
            if href and is_product_link(href) and href not in links:
                links.append(href)
        if links:
            break

    # 2) план Б: берем все ссылки на странице и фильтруем эвристикой
    if not links:
        for a in r.html.find("a[href]"):
            href = abs_url(a.attrs.get("href", ""))
            if href and is_product_link(href) and href not in links:
                links.append(href)

    # пагинация
    next_el = r.html.find('a[rel="next"]', first=True) or r.html.find(".pagination-next a, .next a", first=True)
    next_url = abs_url(next_el.attrs["href"]) if next_el and next_el.attrs.get("href") else None

    print(f"[LIST] page {page_idx}: links={len(links)} next={'yes' if next_url else 'no'}")
    return links, next_url


# ---------- парсинг товара ----------
def parse_product(url: str, idx: int) -> Optional[Product]:
    r = render(url)
    if not r:
        return None

    if SAVE_DEBUG and idx <= 5:
        save_file(f"debug_product_{idx}.html", r.html.html or "")

    title_el = (r.html.find("h1.product-name", first=True)
                or r.html.find("h1[itemprop='name']", first=True)
                or r.html.find("h1", first=True))
    title = norm(title_el.text if title_el else "")

    price_el = (r.html.find(".current-price", first=True)
                or r.html.find(".product-prices .price", first=True)
                or r.html.find("span[itemprop='price']", first=True)
                or r.html.find(".price", first=True))
    price = norm(price_el.text if price_el else "")

    img_el = (r.html.find("img.js-qv-product-cover", first=True)
              or r.html.find(".product-cover img", first=True)
              or r.html.find("img[itemprop='image']", first=True)
              or r.html.find('meta[property="og:image"]', first=True))
    image = ""
    if img_el:
        image = img_el.attrs.get("src") or img_el.attrs.get("content") or img_el.attrs.get("data-src") or ""
        image = abs_url(image)

    crumbs = r.html.find(".breadcrumbs a, ol.breadcrumbs a, ul.breadcrumbs a, nav.breadcrumb a")
    category = ""
    if crumbs:
        category = norm(crumbs[-2].text if len(crumbs) >= 2 else crumbs[-1].text)

    desc_el = r.html.find("#description, .product-description, [itemprop='description']", first=True)
    description = norm(desc_el.text if desc_el else "")

    # fallback из JSON‑LD
    if not title or not price:
        for sc in r.html.find('script[type="application/ld+json"]'):
            try:
                import json
                data = json.loads(sc.text)
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict) and data.get("@type") in ("Product", "Bike", "Thing"):
                    title = title or norm(data.get("name") or "")
                    offers = data.get("offers")
                    if isinstance(offers, list) and offers:
                        offers = offers[0]
                    if isinstance(offers, dict):
                        price = price or str(offers.get("price", "")).strip()
            except Exception:
                pass

    if not title:
        print(f"[WARN] skip (no title): {url}")
        return None

    return Product(title=title, price=price, link=url, image=image, category=category, description=description)


# ---------- обход ----------
def crawl() -> List[Product]:
    items: List[Product] = []
    seen = set()

    page = 1
    page_url = abs_url(f"{START_PATH}?page={page}")

    while page_url:
        print(f"[PAGE] {page_url}")
        links, next_url = parse_list(page_url, page)

        if not links:
            print("[INFO] Товары не найдены на странице — завершаю.")
            break

        new_links = [u for u in links if u not in seen]
        for u in new_links:
            seen.add(u)

        print(f"[INFO] к обработке: {len(new_links)}")
        for i, link in enumerate(new_links, 1):
            print(f"  [{i}/{len(new_links)}] {link}")
            prod = parse_product(link, idx=i)
            if prod:
                items.append(prod)
            time.sleep(SLEEP)

        if MAX_PAGES and page >= MAX_PAGES:
            print("[INFO] Достигнут лимит страниц.")
            break

        page += 1
        page_url = next_url
        time.sleep(SLEEP)

    return items


def main():
    t0 = time.time()
    products = crawl()
    if products:
        df = pd.DataFrame([asdict(p) for p in products])
        df.to_csv("output.csv", index=False)
        df.to_excel("output.xlsx", index=False)
        print(f"[OK] Сохранено {len(df)} товаров -> output.csv, output.xlsx")
    else:
        print("Пусто — сохранять нечего.")
    print(f"⏱ За {time.time() - t0:.1f} сек.")


if __name__ == "__main__":
    main()

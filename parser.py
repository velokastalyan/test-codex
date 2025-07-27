import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

BASE_URL = "https://sprint-rowery.pl/rowery"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def parse_page(url):
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    for product in soup.select("div.product-item-info"):
        title_el = product.select_one("a.product-item-link")
        price_el = product.select_one("span.price")
        title = title_el.get_text(strip=True) if title_el else "Без названия"
        price = price_el.get_text(strip=True) if price_el else "Без цены"
        link = title_el["href"] if title_el else ""
        items.append({"title": title, "price": price, "link": link})
    # find pagination links
    next_page = soup.select_one("a.action.next")
    next_url = urljoin(BASE_URL, next_page["href"]) if next_page else None
    return items, next_url


def parse_all():
    url = BASE_URL
    all_items = []
    while url:
        print(f"Fetching {url}")
        items, url = parse_page(url)
        all_items.extend(items)
    return all_items


def save_data(items):
    df = pd.DataFrame(items)
    df.to_excel("products.xlsx", index=False)
    df.to_csv("products.csv", index=False)


if __name__ == "__main__":
    data = parse_all()
    save_data(data)

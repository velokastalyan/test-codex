import requests
from bs4 import BeautifulSoup

URL = "https://sprint-rowery.pl/rowery"
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(URL, headers=HEADERS)
soup = BeautifulSoup(response.text, "html.parser")

products = soup.select("div.product-wrapper")

for i, product in enumerate(products, 1):
    title_elem = product.select_one("a.product-name")
    price_elem = product.select_one("span.price")

    title = title_elem.get_text(strip=True) if title_elem else "Без названия"
    link = "https://sprint-rowery.pl" + title_elem['href'] if title_elem else "—"
    price = price_elem.get_text(strip=True) if price_elem else "Без цены"

    print(f"{i}. {title}\n   Цена: {price}\n   Ссылка: {link}\n")


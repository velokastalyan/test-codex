import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd

base_url = "https://sprint-rowery.pl"
start_url = f"{base_url}/rowery"

headers = {
    "User-Agent": "Mozilla/5.0"
}

all_products = []

def parse_product_card(card):
    try:
        title = card.select_one("h3.product-title").text.strip()
        price = card.select_one("span.price").text.strip()
        url = base_url + card.select_one("a")["href"]
        image = card.select_one("img")["src"]
        return {"Название": title, "Цена": price, "Ссылка": url, "Фото": image}
    except Exception as e:
        print("Ошибка в карточке товара:", e)
        return None

def parse_page(url):
    print(f"Парсинг страницы: {url}")
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    products = soup.select("div.products div.product")
    for card in products:
        data = parse_product_card(card)
        if data:
            all_products.append(data)

    # Пагинация
    next_link = soup.select_one("a.next")
    if next_link:
        next_url = base_url + next_link["href"]
        parse_page(next_url)

# Запуск
parse_page(start_url)

# Сохранение в CSV и Excel
csv_file = "sprint_rowery_output.csv"
xlsx_file = "sprint_rowery_output.xlsx"

df = pd.DataFrame(all_products)
df.to_csv(csv_file, index=False)
df.to_excel(xlsx_file, index=False)

print(f"✅ Готово. Сохранено в файлы: {csv_file}, {xlsx_file}")

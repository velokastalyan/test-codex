import requests
from bs4 import BeautifulSoup
import pandas as pd

base_url = "https://sprint-rowery.pl"
start_url = f"{base_url}/rowery"

headers = {
    "User-Agent": "Mozilla/5.0"
}

all_products = []

def parse_product_card(card):
    try:
        title = card.select_one("h3.product-item__name__heading").text.strip()
        price = card.select_one("div.product-price-final-price").text.strip()
        link_tag = card.select_one("a")
        url = base_url + link_tag["href"] if link_tag and link_tag.has_attr("href") else ""
        image_tag = card.select_one("img")
        image = image_tag["src"] if image_tag and image_tag.has_attr("src") else ""

        return {
            "Название": title,
            "Цена": price,
            "Ссылка": url,
            "Фото": image
        }
    except Exception as e:
        print("Ошибка в карточке товара:", e)
        return None

def parse_page(url):
    print(f"Парсинг страницы: {url}")
    res = requests.get(url, headers=headers)
print(f"Статус ответа: {res.status_code}")
    res.raise_for_status()
    
    # ⬇⬇⬇ Сохраняем HTML для отладки ⬇⬇⬇
    with open("debug.html", "w", encoding="utf-8") as f:
        f.write(res.text)
    
    soup = BeautifulSoup(res.text, "html.parser")
    products = soup.select("div.product-item")

    for card in products:
        data = parse_product_card(card)
        if data:
            all_products.append(data)

    # Пагинация
    next_link = soup.select_one("a.next")
    if next_link and next_link.has_attr("href"):
        next_url = base_url + next_link["href"]
        parse_page(next_url)

# Запуск
parse_page(start_url)

# Сохранение
csv_file = "sprint_rowery_output.csv"
xlsx_file = "sprint_rowery_output.xlsx"

df = pd.DataFrame(all_products)
df.to_csv(csv_file, index=False)
df.to_excel(xlsx_file, index=False)

print(f"\nГотово. Сохранено в файлы: {csv_file}, {xlsx_file}")

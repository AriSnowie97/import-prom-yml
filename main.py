import os
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
import threading
import time
import hashlib
import io

from flask import Flask, Response
import gspread
from google.oauth2 import service_account

# ----------------------------
# Flask app
# ----------------------------
app = Flask(__name__)

# ----------------------------
# GOOGLE SHEETS AUTH
# ----------------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds_json = os.getenv("GOOGLE_CREDENTIALS")
if not creds_json:
    raise Exception("Переменная окружения GOOGLE_CREDENTIALS не найдена!")

creds_info = json.loads(creds_json)
creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client = gspread.authorize(creds)

# ----------------------------
# Параметры таблиц
# ----------------------------
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1BBq14rbObiYNOWZGKkNsxW6_NZ4zx8t38zvnRPV0tPU/edit"
RU_SHEET = "catalog_new_ru"
UA_SHEET = "catalog_new"

# ----------------------------
# Хранилище последнего XML
# ----------------------------
latest_xml = None
xml_lock = threading.Lock()  # для потокобезопасности

# ----------------------------
# Функции для генерации YML
# ----------------------------
def add_cdata(element, text):
    element.text = f"<![CDATA[{text}]]>"

def extract_group_id(product_code, counter_dict):
    digits = ''.join(filter(str.isdigit, product_code))
    if digits:
        group_id = int(digits) % 2147483647
        if group_id == 0:
            group_id = 1
    else:
        if product_code not in counter_dict:
            counter_dict[product_code] = len(counter_dict) + 1
        group_id = counter_dict[product_code]
    return str(group_id)

def parse_and_generate_yml():
    global latest_xml
    counter_dict = {}
    try:
        sheet_ru = client.open_by_url(SPREADSHEET_URL).worksheet(RU_SHEET)
        sheet_ua = client.open_by_url(SPREADSHEET_URL).worksheet(UA_SHEET)
        data_ru = sheet_ru.get_all_records()
        data_ua = sheet_ua.get_all_records()
        ua_mapping = {str(row['Product Code']).strip(): row for row in data_ua}
        grouped = defaultdict(list)
        for row in data_ru:
            grouped[str(row['Product Code']).strip()].append(row)
    except Exception as e:
        print(f"Ошибка доступа к Google Sheets: {e}")
        return

    yml_catalog = ET.Element('yml_catalog', date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml_catalog, 'shop')
    ET.SubElement(shop, 'name').text = 'Ego Textile'
    ET.SubElement(shop, 'company').text = 'EGO TEXTILE'
    ET.SubElement(shop, 'url').text = 'https://ego-textile.com.ua/'
    currencies = ET.SubElement(shop, 'currencies')
    ET.SubElement(currencies, 'currency', id='UAH', rate='1')
    categories = ET.SubElement(shop, 'categories')
    ET.SubElement(categories, 'category', id='40601').text = 'Комплекти постільної білизни'
    offers = ET.SubElement(shop, 'offers')

    for product_code, items in grouped.items():
        try:
            if not items:
                continue
            main_item = items[0]
            name_ru = str(main_item.get('Name', '')).strip()
            description_ru = str(main_item.get('Description', '')).strip()
            price = str(main_item.get('Price', '')).replace('.0','').strip()
            country = str(main_item.get('Country of manufacture', '')).strip()
            producer = str(main_item.get('Producer', '')).strip()
            fabric_type = str(main_item.get('Fabric type', '')).strip()
            density_raw = str(main_item.get('Density', '')).strip()
            subcategory = str(main_item.get('Subcategory', '')).strip()
            ua_row = ua_mapping.get(product_code, {})
            name_ua = str(ua_row.get('Name', '')).strip()
            description_ua = str(ua_row.get('Description', '')).strip()
            density_digits = ''.join(filter(str.isdigit, density_raw))
            density = density_digits[:3] if len(density_digits)>=3 else density_digits
            if not all([product_code, name_ru, price]):
                continue
            group_id = extract_group_id(product_code, counter_dict)
            if not subcategory:
                subcategory = "Основной комплект"

            # Главный оффер
            offer_main = ET.SubElement(
                offers, 'offer',
                id=f"{product_code}_main",
                available='true',
                group_id=group_id
            )
            ET.SubElement(offer_main, 'name').text = name_ru
            desc_elem = ET.SubElement(offer_main, 'description')
            add_cdata(desc_elem, f"<h2>Описание комплекта</h2><p>{description_ru}</p>")
            ET.SubElement(offer_main, 'name_ua').text = name_ua
            desc_ua_elem = ET.SubElement(offer_main, 'description_ua')
            add_cdata(desc_ua_elem, f"<h2>Опис комплекту</h2><p>{description_ua}</p>")
            ET.SubElement(offer_main, 'price').text = price
            ET.SubElement(offer_main, 'currencyId').text = 'UAH'
            ET.SubElement(offer_main, 'categoryId').text = '40601'
            ET.SubElement(offer_main, 'vendor').text = producer
            ET.SubElement(offer_main, 'country_of_origin').text = country
            ET.SubElement(offer_main, 'vendorCode').text = product_code
            for i in range(1, 11):
                photo = main_item.get(f'Main photo {i}', '')
                if photo.strip():
                    ET.SubElement(offer_main, 'picture').text = photo.strip()
            if fabric_type:
                ET.SubElement(offer_main, 'param', name='Тип тканини').text = fabric_type
            if density:
                ET.SubElement(offer_main, 'param', name='Плотність(г/м2)').text = density
            ET.SubElement(offer_main, 'param', name='Тип комплекта').text = subcategory

            # Вариации
            for idx, var_item in enumerate(items[1:], start=1):
                subcategory_var = str(var_item.get('Subcategory','')).strip() or f"Вариант {idx}"
                price_var = str(var_item.get('Price','')).replace('.0','').strip() or price
                offer_var_id = f"{product_code}_{hashlib.md5(subcategory_var.encode()).hexdigest()[:8]}"
                offer_var = ET.SubElement(
                    offers, 'offer',
                    id=offer_var_id,
                    available='true',
                    group_id=group_id
                )
                ET.SubElement(offer_var, 'name').text = f"{name_ru} {subcategory_var}".strip()
                ET.SubElement(offer_var, 'price').text = price_var
                ET.SubElement(offer_var, 'currencyId').text = 'UAH'
                ET.SubElement(offer_var, 'categoryId').text = '40601'
                ET.SubElement(offer_var, 'vendor').text = producer
                ET.SubElement(offer_var, 'vendorCode').text = product_code
                for i in range(1,11):
                    photo = var_item.get(f'Main photo {i}','')
                    if photo.strip():
                        ET.SubElement(offer_var, 'picture').text = photo.strip()
                ET.SubElement(offer_var, 'param', name='Тип комплекта').text = subcategory_var
        except Exception as e:
            print(f"Ошибка обработки {product_code}: {e}")
            continue

    # ----------------------------
    # Сохранение с xml_declaration
    # ----------------------------
    tree = ET.ElementTree(yml_catalog)
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    xml_str = buf.getvalue()

    with xml_lock:
        latest_xml = xml_str
    print(f"{datetime.now()}: YML обновлён.")

# ----------------------------
# Фоновый поток обновления каждые 4 часа
# ----------------------------
def background_updater():
    while True:
        parse_and_generate_yml()
        time.sleep(4*60*60)  # 4 часа

threading.Thread(target=background_updater, daemon=True).start()

# ----------------------------
# Flask route
# ----------------------------
@app.route("/")
def index():
    with xml_lock:
        if latest_xml:
            return Response(latest_xml, mimetype="application/xml")
        else:
            return "YML ещё не сгенерирован", 503

# ----------------------------
# Запуск Flask
# ----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

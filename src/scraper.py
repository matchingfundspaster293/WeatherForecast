import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import re
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from . import config

MONTHS_RU = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня',
    7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
}

def generate_url_list():
    urls = []
    date_range = pd.date_range(start=config.SCRAPE_START_DATE, end=config.SCRAPE_END_DATE)
    for date in date_range:
        day, month_name, year = date.day, MONTHS_RU[date.month], date.year
        urls.append(urljoin(config.BASE_URL, f"/{day}-{month_name}#{year}"))
    print(f"Сгенерировано {len(urls)} URL для скрапинга.")
    return urls

def parse_data_from_js(soup, page_url):
    """Извлекает ВСЕ числовые данные ИСКЛЮЧИТЕЛЬНО из JS-объекта 'mart.forecastMap'."""
    records = []
    script_tag = soup.find('script', string=re.compile(r"mart\.forecastMap\.set"))
    if not script_tag: return []
    
    year_match = re.search(r'#(\d{4})', page_url)
    if not year_match: return []
    year = year_match.group(1)
    
    data_match = re.search(r"mart\.forecastMap\.set\('" + year + r"',\s*(\[\[.*?\]\])\);", script_tag.string)
    if not data_match: return []

    try: data_list = json.loads(data_match.group(1))
    except json.JSONDecodeError: return []
        
    for item in data_list:
        # Убедимся, что в записи достаточно данных
        if len(item) < 7: continue
        records.append({
            'datetime': pd.to_datetime(item[0], unit='s'),
            'temperature': item[1],
            'pressure': item[2],
            'wind_speed': item[3],
            'wind_direction_deg': item[4], # Направление ветра в градусах
            'cloud_cover_percent': item[6] # Облачность в процентах
        })
            
    return records

def scrape_url(url):
    time.sleep(config.REQUEST_DELAY * random.uniform(0.5, 1.5))
    for attempt in range(config.MAX_RETRIES):
        try:
            response = requests.get(url, headers=config.HEADERS, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return parse_data_from_js(soup, url)
        except requests.RequestException:
            if attempt + 1 == config.MAX_RETRIES:
                print(f"Не удалось скачать {url} после {config.MAX_RETRIES} попыток.")
                return None
            time.sleep(config.BACKOFF_FACTOR * (2 ** attempt))
    return None

def run_parallel_scrape():
    urls = generate_url_list()
    all_records = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_url = {executor.submit(scrape_url, url): url for url in urls}
        processed_count = 0
        total_urls = len(urls)
        for future in as_completed(future_to_url):
            processed_count += 1
            daily_data = future.result()
            if daily_data:
                all_records.extend(daily_data)
            print(f"Обработано {processed_count}/{total_urls} URL. Собрано 'сырых' записей: {len(all_records)}", end='\r')
    print("\nПараллельный скрапинг завершен.")
    if not all_records: return pd.DataFrame()
    df = pd.DataFrame(all_records)
    return df.sort_values('datetime').drop_duplicates(subset=['datetime']).reset_index(drop=True)

def aggregate_data(df_raw: pd.DataFrame):
    """
    Агрегирует все признаки по периодам дня: утро, день, вечер.
    """
    df = df_raw.copy().dropna(subset=['datetime'])
    if df.empty: return pd.DataFrame()

    df['date'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    
    # Определяем период для каждой записи
    def get_period(hour):
        for period, hours in config.PERIOD_MAPPING.items():
            if hour in hours: return period
        return None
    df['period'] = df['hour'].apply(get_period)
    
    # Отфильтровываем записи, не попавшие в наши периоды (например, ночные)
    df_filtered = df.dropna(subset=['period'])
    if df_filtered.empty: return pd.DataFrame()


    # Определяем все признаки, которые мы хотим агрегировать
    features_to_aggregate = [
        'temperature',
        'pressure',
        'wind_speed',
        'wind_direction_deg',
        'cloud_cover_percent'
    ]

    
    df_pivoted = df_filtered.pivot_table(
        index='date',                 # Группируем по дням
        columns='period',             # Создаем колонки для 'morning', 'day', 'evening'
        values=features_to_aggregate, # Применяем ко всем признакам
        aggfunc='mean'                # Используем среднее для агрегации
    )
    
    # Теперь у нас мульти-индексные колонки (например, ('temperature', 'morning')).
    # Превратим их в плоские имена ('temperature_morning').
    df_pivoted.columns = [f'{feature}_{period}' for feature, period in df_pivoted.columns]
    
    df_final = df_pivoted.reset_index()
    df_final['date'] = pd.to_datetime(df_final['date'])
    
    # Удаляем строки, если для какого-то дня не нашлось данных для всех периодов
    return df_final.dropna(how='any')
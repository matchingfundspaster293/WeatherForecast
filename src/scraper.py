import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import re
import json
from . import config

def parse_data_from_script(soup, page_url):
    """
    Извлекает данные о погоде напрямую из JavaScript переменной 'mart.forecastMap' на странице.
    Это самый надежный метод.
    """
    records = []
    
    # 1. Находим тег <script>, содержащий нужные нам данные
    script_tag = soup.find('script', text=re.compile(r"mart\.forecastMap\.set"))
    if not script_tag:
        print("Не найден <script> с данными 'mart.forecastMap'.")
        return []

    # 2. Извлекаем год из URL, чтобы знать, какие данные искать
    year_match = re.search(r'#(\d{4})', page_url)
    if not year_match:
        return []
    year = year_match.group(1)

    # 3. Используем регулярное выражение для извлечения списка данных для нужного года
    data_match = re.search(r"mart\.forecastMap\.set\('" + year + r"',\s*(\[\[.*?\]\])\);", script_tag.string)
    if not data_match:
        print(f"Не найдены данные для года {year} в <script>.")
        return []

    # 4. Преобразуем найденную строку в Python-список
    try:
        data_list = json.loads(data_match.group(1))
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON для года {year}.")
        return []
        
    # 5. Преобразуем список в нужный нам формат (DataFrame)
    # Формат данных: [timestamp_ms, temp, pressure, wind_speed, ...]
    for item in data_list:
        if len(item) < 4: continue # Проверка на корректность записи
        
        records.append({
            # item[0] - это timestamp в миллисекундах
            'datetime': pd.to_datetime(item[0], unit='ms'),
            'temperature': item[1],
            'pressure': item[2],
            'wind_speed': item[3],
        })
        
    return records

def run_full_scrape():
    """Запускает полный цикл скрапинга, пока не дойдет до последней страницы."""
    all_records = []
    current_url = urljoin(config.BASE_URL, config.START_URL_PATH)
    
    page_count = 0
    while current_url:
        page_count += 1
        print(f"[{page_count}] Скрапинг страницы: {current_url}")
        
        try:
            response = requests.get(current_url, headers=config.HEADERS, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Ошибка запроса: {e}. Прерывание скрапинга.")
            break
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Используем нашу новую, надежную функцию парсинга
        daily_data = parse_data_from_script(soup, current_url)
        
        if not daily_data:
            print("Не удалось извлечь данные. Завершение скрапинга.")
            break
        
        all_records.extend(daily_data)
        
        # Поиск ссылки на предыдущую страницу все еще нужен
        next_page_path_tag = soup.select_one('div.navi a.prev-month')
        if next_page_path_tag and next_page_path_tag.has_attr('href'):
            current_url = urljoin(config.BASE_URL, next_page_path_tag['href'])
        else:
            print("Не найдена ссылка на предыдущий день. Конец архива.")
            current_url = None
        
        time.sleep(config.REQUEST_DELAY)
        
    if not all_records:
        print("В результате скрапинга не было собрано ни одной записи.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    # Данные могут приходить не по порядку для одного дня, сортируем
    df = df.sort_values('datetime').drop_duplicates(subset=['datetime']).reset_index(drop=True)
    return df

# Функция aggregate_data остается без изменений
def aggregate_data(df_raw: pd.DataFrame):
    """Агрегирует 3-часовые данные в дневные по периодам: утро, день, вечер."""
    df = df_raw.copy()
    df['date'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    
    def get_period(hour):
        for period, hours in config.PERIOD_MAPPING.items():
            if hour in hours:
                return period
        return None
        
    df['period'] = df['hour'].apply(get_period)
    df = df.dropna(subset=['period'])
    
    df_temp = df.pivot_table(index='date', columns='period', values='temperature', aggfunc='mean')
    df_temp.columns = [f'temp_{col}' for col in df_temp.columns]
    
    df_features = df.groupby('date').agg(
        pressure_mean=('pressure', 'mean'),
        wind_speed_mean=('wind_speed', 'mean')
    )
    
    df_final = df_temp.join(df_features).reset_index()
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final = df_final.dropna()
    
    return df_final
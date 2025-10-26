# --- Scraper Settings ---
BASE_URL = "https://nizhniy-tagil.nuipogoda.ru"
# Начинаем с актуальной даты и идем в прошлое
START_URL_PATH = "/30-сентября#2025" 
CITY_NAME = "Nizhny Tagil"
REQUEST_DELAY = 1  # Задержка в секундах между запросами, чтобы не перегружать сервер
REQUEST_TIMEOUT = 15 # Таймаут запроса в секундах
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- Database Settings ---
DB_PATH = "data/weather.db"
TABLE_NAME = "weather_data"

# --- Data Aggregation Settings ---
PERIOD_MAPPING = {
    'morning': [6, 9],
    'day': [12, 15],
    'evening': [18, 21]
}
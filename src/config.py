# --- Scraper Settings ---
BASE_URL = "https://nizhniy-tagil.nuipogoda.ru"
REQUEST_DELAY = 1.0 
REQUEST_TIMEOUT = 20
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MAX_RETRIES = 3
BACKOFF_FACTOR = 5

SCRAPE_START_DATE = "2010-11-01"
SCRAPE_END_DATE = "2025-09-30"
MAX_WORKERS = 10 # Количество одновременных потоков для скрапинга

# --- Database Settings ---
DB_PATH = "data/weather.db"
TABLE_NAME = "weather_data"

# --- Data Aggregation Settings ---
PERIOD_MAPPING = {
    'morning': [6, 9],
    'day': [12, 15],
    'evening': [18, 21]
}
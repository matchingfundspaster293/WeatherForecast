from .scraper import run_full_scrape, aggregate_data
from .database import save_data_to_db

def main():
    """Основная функция для запуска всего пайплайна сбора данных."""
    print("--- Начало процесса сбора данных ---")
    
    # Шаг 1: Запуск скрапера для получения "сырых" 3-часовых данных
    raw_data = run_full_scrape()
    
    if raw_data.empty:
        print("Скрапинг не вернул данных. Процесс завершен.")
        return
        
    print(f"\nСкрапинг завершен. Получено {len(raw_data)} записей.")
    
    # Шаг 2: Агрегация данных по периодам дня
    print("Начало агрегации данных...")
    aggregated_data = aggregate_data(raw_data)
    print(f"Агрегация завершена. Получено {len(aggregated_data)} дневных записей.")
    
    # Шаг 3: Сохранение результатов в базу данных
    print("Сохранение данных в БД...")
    save_data_to_db(aggregated_data)
    
    print("\n--- Процесс сбора данных успешно завершен! ---")

if __name__ == "__main__":
    main()
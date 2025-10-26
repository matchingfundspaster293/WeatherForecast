import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from . import config

def get_db_engine():
    """Создает и возвращает SQLAlchemy engine."""
    return create_engine(f'sqlite:///{config.DB_PATH}')

def save_data_to_db(df: pd.DataFrame):
    """Сохраняет DataFrame в базу данных, перезаписывая старую таблицу."""
    if df.empty:
        print("DataFrame пуст. Сохранение в базу данных отменено.")
        return
        
    engine = get_db_engine()
    df.to_sql(config.TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"Данные успешно сохранены в таблицу '{config.TABLE_NAME}' в файле '{config.DB_PATH}'.")
    print(f"Всего сохранено {len(df)} записей.")
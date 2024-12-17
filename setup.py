import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# Подключение к базе данных SQLite
def connect_to_db(db_name="database.db"):
    """
    Функция для подключения к базе данных SQLite.
    Возвращает объект соединения с базой данных.
    """
    try:
        conn = sqlite3.connect(db_name)
        print("Подключение к базе данных установлено.")
        return conn
    except sqlite3.Error as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None

# Создание таблицы (если не существует)
def create_table(conn):
    """
    Функция для создания таблицы в базе данных.
    Если таблица уже существует, то она не будет пересоздана.
    """
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        age INTEGER,
        email TEXT UNIQUE
    );
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()  # Подтверждаем изменения в базе данных
        print("Таблица 'users' успешно создана.")
    except sqlite3.Error as e:
        print(f"Ошибка при создании таблицы: {e}")

# Вставка данных в таблицу
def insert_user_data(conn, user_data):
    """
    Функция для вставки данных о пользователях в таблицу 'users'.
    :param conn: соединение с базой данных
    :param user_data: список кортежей с данными пользователей
    """
    insert_sql = "INSERT INTO users (name, age, email) VALUES (?, ?, ?)"
    try:
        cursor = conn.cursor()
        cursor.executemany(insert_sql, user_data)
        conn.commit()
        print(f"{len(user_data)} пользователя успешно добавлены.")
    except sqlite3.Error as e:
        print(f"Ошибка при вставке данных: {e}")

# Извлечение всех данных из таблицы
def fetch_all_users(conn):
    """
    Функция для извлечения всех пользователей из таблицы 'users'.
    Возвращает DataFrame с данными.
    """
    select_sql = "SELECT * FROM users"
    try:
        df = pd.read_sql(select_sql, conn)  # Используем pandas для извлечения данных
        print("Данные о пользователях успешно извлечены.")
        return df
    except sqlite3.Error as e:
        print(f"Ошибка при извлечении данных: {e}")
        return None

# Пример работы с базой данных
def main():
    conn = connect_to_db()  # Подключаемся к базе данных
    if conn:
        create_table(conn)  # Создаем таблицу, если её нет

        # Пример данных для вставки
        users = [
            ('Алексей', 28, 'aleksey@example.com'),
            ('Мария', 34, 'maria@example.com'),
            ('Иван', 45, 'ivan@example.com')
        ]
        insert_user_data(conn, users)  # Вставляем данные

        # Извлекаем все данные о пользователях и выводим их
        users_df = fetch_all_users(conn)
        if users_df is not None:
            print(users_df)

        conn.close()  # Закрываем соединение с базой данных

# Запуск основной программы
if __name__ == "__main__":
    main()

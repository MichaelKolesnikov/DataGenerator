import psycopg2

DB_CONFIG = {
    'dbname': 'test_db',  # Имя базы данных (по умолчанию 'postgres')
    'user': 'root',    # Имя пользователя (по умолчанию 'postgres')
    'password': 'root',  # Пароль, который вы установили
    'host': 'localhost',   # Хост, на котором запущен PostgreSQL (localhost, если контейнер на локальной машине)
    'port': '54320',
}

def main():
    try:
        with open('code.sql') as f:
            create_command = f.read()
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(create_command)

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
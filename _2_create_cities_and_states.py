import psycopg2


def create_country(cur):
    cur.execute("insert into country (id, name) values (0, 'Россия');")


def create_states(cur, state_id_to_name):
    cur.executemany(
        "insert into state (id, name) values (%s, %s);",
        [(index, state_id_to_name[index]) for index in range(len(state_id_to_name))]
    )


def create_cities(cur, city_id_to_name):
    cur.executemany(
        "insert into city (id, name) values (%s, %s);",
        [(index, city_id_to_name[index]) for index in range(len(city_id_to_name))]
    )


def get_cities_states_info():
    with open("cities_info.txt") as f:
        city_id_to_name = list(f.readline().split(';'))
        city_id_to_state_id = list(map(int, f.readline().split(';')))
        state_id_to_name = list(f.readline().split(';'))
        return city_id_to_name, city_id_to_state_id, state_id_to_name


def main():
    from config import db_config

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            city_id_to_name, city_id_to_state_id, state_id_to_name = get_cities_states_info()
            create_country(cur)
            create_states(cur, state_id_to_name)
            create_cities(cur, city_id_to_name)

if __name__ == "__main__":
    main()

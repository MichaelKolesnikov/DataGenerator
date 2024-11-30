from functions import measure_time
from faker import Faker
import psycopg2


@measure_time
def create_addresses_for_schools(cur, faker: Faker, count, city_id_to_state_id):
    city_count = len(city_id_to_state_id)
    final_data = []
    for address_id in range(count):
        city_id = address_id % city_count
        address = (
            address_id,
            faker.street_name(),
            city_id,
            city_id_to_state_id[city_id],
            faker.postcode(),
            0,
        )
        final_data.append(address)

    cur.executemany(
        "INSERT INTO Address (id, street, city_id, state_id, postal_code, country_id) VALUES (%s, %s, %s, %s, %s, %s);",
        final_data,
    )


@measure_time
def create_schools(cur, faker, school_count, city_id_to_state_id):
    school_id_to_city_id = create_addresses_for_schools(cur, faker, school_count, city_id_to_state_id)
    school_data = []
    for i in range(school_count):
        school_name = f"Школа №{i}"
        school_data.append(
            (i, school_name, i)
        )
    cur.executemany(
        "INSERT INTO School (id, name, address_id) VALUES (%s, %s, %s);",
        school_data,
    )
    return school_id_to_city_id


from _2_create_cities_and_states import get_cities_states_info


def main():
    from config import db_config, school_count
    faker = Faker("ru_RU")

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            city_id_to_name, city_id_to_state_id, state_id_to_name = get_cities_states_info()
            create_schools(cur, faker, school_count, city_id_to_state_id)


if __name__ == "__main__":
    main()

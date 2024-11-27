from functions import measure_time
from faker import Faker


@measure_time
def create_addresses_for_schools(cur, faker: Faker, count):
    final_data = []
    address_id = 1
    for _ in range(count):
        address = (
            address_id,
            faker.street_name(),
            faker.city_name(),
            faker.administrative_unit(),
            faker.postcode(),
            "Россия",
        )
        final_data.append(address)
        address_id += 1

    cur.executemany(
        "INSERT INTO Address (id, street, city, state, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s);",
        final_data,
    )


@measure_time
def create_schools(cur, faker, school_count):
    create_addresses_for_schools(cur, faker, school_count)
    school_data = []
    for i in range(school_count):
        school_name = f"Школа №{i + 1}"
        address_id = i + 1
        school_data.append(
            (school_name, address_id)
        )
    cur.executemany(
        "INSERT INTO School (name, address_id) VALUES (%s, %s) RETURNING id;",
        school_data,
    )
    cur.execute("""
                SELECT school.id, address.city, address.state
                FROM school 
                JOIN address ON school.address_id = address.id;
            """)
    school_id_to_data = {school_id: [city, state] for school_id, city, state in cur.fetchall()}
    return school_id_to_data

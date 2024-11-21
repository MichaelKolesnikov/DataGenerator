import os
import random
import json

from dotenv import load_dotenv

load_dotenv()

from faker import Faker
import psycopg2


def create_addresses(cur, faker: Faker, count):
    final_data = []
    for _ in range(count):
        address = (
            faker.street_name(),
            faker.city_name(),
            faker.administrative_unit(),
            faker.postcode(),
            faker.country(),
        )
        final_data.append(address)

    cur.executemany(
        "INSERT INTO Address (street, city, state, postal_code, country) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
        final_data,
    )
    cur.execute("SELECT id FROM Address;")
    address_ids = [row[0] for row in cur.fetchall()]
    return address_ids

def parse_subject_mapping(file_path):
    points = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        current_subject = None
        for line in file.read().split('\n'):
            if not line:
                continue
            if line[0].isalpha():
                current_subject = line
                points[current_subject] = {}
                continue

            tasks, price = line.split()
            if '-' in tasks:
                task1, task2 = map(int, tasks.split('-'))
            else:
                task1 = task2 = int(tasks)
            for i in range(task1, task2 + 1):
                points[current_subject][i] = price

    return points
def create_subjects(cur, subjects_points):
    subjects_data = []
    for subject in subjects_points:
        score_mapping = subjects_points[subject]
        score_mapping_json = json.dumps(score_mapping, ensure_ascii=False)
        subjects_data.append((subject, score_mapping_json))
    cur.executemany(
        "INSERT INTO Subject (name, ScoreMapping_JSON) VALUES (%s, %s) RETURNING id;",
        subjects_data,
    )

    cur.execute("SELECT id FROM Subject;")
    subject_ids = [row[0] for row in cur.fetchall()]
    return subject_ids

def create_schools(cur, faker: Faker, school_count, address_ids):
    school_data = []
    for i in range(school_count):
        school_name = f"Школа №{i + 1}"
        address_id = address_ids.pop()
        school_data.append(
            (school_name, address_id)
        )
    cur.executemany(
        "INSERT INTO School (name, address_id) VALUES (%s, %s) RETURNING id;",
        school_data,
    )
    cur.execute("SELECT id FROM school;")
    school_ids = [row[0] for row in cur.fetchall()]
    return school_ids

def create_teachers(cur, faker: Faker, teacher_count, subject_ids, school_ids):
    teachers_data = []
    for _ in range(teacher_count):
        random.choice(school_ids)
        teachers_data.append(
            [
                faker.name(),
                0,
                random.choice(school_ids),
                random.choice(subject_ids),
            ]
        )

    cur.execute("""
            SELECT school.id, address.city 
            FROM school 
            JOIN address ON school.address_id = address.id;
        """)
    results = {i: j for i, j in cur.fetchall()}
    addresses_data = []
    for i, school_id in enumerate(t[2] for t in teachers_data):
        address = (
            faker.street_name(),
            results[school_id],
            faker.administrative_unit(),
            faker.postcode(),
            faker.country(),
        )
        addresses_data.append(address)

    values = ', '.join(cur.mogrify("(%s, %s, %s, %s, %s)", address).decode('utf-8') for address in addresses_data)
    query = f"""
    WITH inserted AS (
        INSERT INTO Address (street, city, state, postal_code, country)
        VALUES {values}
        RETURNING id
    )
    SELECT id FROM inserted;
    """
    cur.execute(query)
    inserted_ids = [row[0] for row in cur.fetchall()]

    for i in range(len(teachers_data)):
        teachers_data[i][1] = inserted_ids[i]

    cur.executemany(
        """INSERT INTO Teacher (FullName, address_id, school_id, subject_id) VALUES (%s, %s, %s, %s);""",
        teachers_data,
    )
    cur.execute(
        "select id from teacher;"
    )
    teacher_ids = [row[0] for row in cur.fetchall()]
    return teacher_ids

def main():
    faker = Faker("ru_RU")
    teacher_count = int(os.getenv('TEACHER_COUNT'))
    school_count = int(os.getenv('SCHOOL_COUNT'))

    db_config = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            address_ids = create_addresses(cur, faker, school_count)
            subjects_points = parse_subject_mapping('subject_data.txt')
            subject_ids = create_subjects(cur, subjects_points)
            school_ids = create_schools(cur, faker, school_count, address_ids)
            teacher_ids = create_teachers(cur, faker, teacher_count, subject_ids, school_ids)


    return 0

def main2():
    faker = Faker("ru_RU")
    teacher_count = int(os.getenv('TEACHER_COUNT'))
    school_count = int(os.getenv('SCHOOL_COUNT'))

    db_config = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT school.id, address.city 
                        FROM school 
                        JOIN address ON school.address_id = address.id;
                    """)



if __name__ == "__main__":
    main()

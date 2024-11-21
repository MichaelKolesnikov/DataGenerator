import copy
import os
import random
import json
from multiprocessing import Pool, Manager
from functools import partial

from dotenv import load_dotenv
load_dotenv()

from faker import Faker
import psycopg2

import time
def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        function_name = func.__name__
        print(f"{function_name} took {elapsed_time:.4f} seconds")
        return result
    return wrapper


@measure_time
def create_addresses(cur, faker: Faker, count):
    final_data = []
    address_id = 1
    for _ in range(count):
        address = (
            address_id,
            faker.street_name(),
            faker.city_name(),
            faker.administrative_unit(),
            faker.postcode(),
            faker.country(),
        )
        final_data.append(address)
        address_id += 1

    cur.executemany(
        "INSERT INTO Address (id, street, city, state, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s);",
        final_data,
    )
    return list(range(1, address_id))

@measure_time
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
@measure_time
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

    cur.execute("SELECT id, name FROM Subject;")
    subject_id_to_name = {row[0]: row[1] for row in cur.fetchall()}
    subject_ids = list(subject_id_to_name.keys())
    return subject_ids, subject_id_to_name
@measure_time
def create_variants(cur, subject_ids, number_of_variants_per_subject):
    variant_data = []
    for subject_id in subject_ids:
        for i in range(number_of_variants_per_subject):
            variant_data.append(
                ((i + 1), subject_id)
            )
    cur.executemany(
        "INSERT INTO Variant (number, subject_id) VALUES (%s, %s);",
        variant_data,
    )
    cur.execute("SELECT id, subject_id FROM variant;")
    variant_id_to_subject_id = {row[0]: row[1] for row in cur.fetchall()}
    return variant_id_to_subject_id
@measure_time
def create_tasks(cur, subject_id_to_name, subjects_points, number_of_variants_per_task: int):
    tasks_data = []
    task_id = 1
    task_id_to_task_data = {}
    for subject_id in subject_id_to_name:
        subject_name = subject_id_to_name[subject_id]
        for task_number in subjects_points[subject_name]:
            for _ in range(number_of_variants_per_task):
                price = subjects_points[subject_name][task_number]
                description = f"{subject_name}-task"
                tasks_data.append(
                    (task_id, task_number, description, price)
                )
                task_id_to_task_data[task_id] = [task_number, subject_name]
                task_id += 1
    cur.executemany(
        "insert into task (id, number, description, price) values (%s, %s, %s, %s);",
        tasks_data,
    )
    task_ids = task_id_to_task_data.keys()
    return task_ids, task_id_to_task_data
@measure_time
def create_links_between_tasks_and_variants(cur, task_id_to_task_data_, variant_id_to_subject_id, subject_id_to_name, subjects_points):
    task_id_to_task_data = copy.deepcopy(task_id_to_task_data_)
    variant_task = {}
    link_data = []
    for variant_id in variant_id_to_subject_id:
        subject_id = variant_id_to_subject_id[variant_id]
        subject_name = subject_id_to_name[subject_id]
        to_remove = []
        for task_id in task_id_to_task_data:
            task_number, subject_name_ = task_id_to_task_data[task_id]
            if variant_id not in variant_task:
                variant_task[variant_id] = []
            if subject_name_ == subject_name and task_number not in variant_task[variant_id]:
                variant_task[variant_id].append(task_number)
                to_remove.append(task_id)
                link_data.append(
                    (task_id, variant_id)
                )

        for id_ in to_remove:
            del task_id_to_task_data[id_]

    cur.executemany(
        "insert into taskwithvariant (task_id, variant_id) values (%s, %s);",
        link_data,
    )

@measure_time
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
    cur.execute("""
                SELECT school.id, address.city, address.state
                FROM school 
                JOIN address ON school.address_id = address.id;
            """)
    school_id_to_data = {school_id: [city, state] for school_id, city, state in cur.fetchall()}
    return school_id_to_data

def create_teachers(db_config, faker: Faker, teacher_count, subject_ids, school_id_to_data, school_count, start, end):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            teachers_data = []
            addresses_data = []
            address_id = school_count + 1 + start
            for teacher_number in range(start, end):
                school_id = random.choice(list(school_id_to_data.keys()))
                address = (
                    address_id,
                    faker.street_name(),
                    school_id_to_data[school_id][0],
                    school_id_to_data[school_id][1],
                    faker.postcode(),
                    faker.country(),
                )
                addresses_data.append(address)
                teachers_data.append(
                    [
                        faker.name(),
                        address_id,
                        school_id,
                        random.choice(subject_ids),
                    ]
                )
                address_id += 1
                if teacher_number % 10000 == 0:
                    print(f"Teacher number: {teacher_number - start}")
                    cur.executemany(
                        "INSERT INTO Address (id, street, city, state, postal_code, country) values (%s, %s, %s, %s, %s, %s);",
                        addresses_data,
                    )
                    cur.executemany(
                        """INSERT INTO Teacher (FullName, address_id, school_id, subject_id) VALUES (%s, %s, %s, %s);""",
                        teachers_data,
                    )
                    addresses_data.clear()
                    teachers_data.clear()

            cur.execute(
                "select id from teacher;"
            )
            teacher_ids = [row[0] for row in cur.fetchall()]
    return teacher_ids
@measure_time
def create_teachers_parallel(db_config, faker: Faker, teacher_count, subject_ids, school_id_to_data, school_count, num_processes):
    with Pool(processes=num_processes) as pool:
        chunk_size = teacher_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        arguments = [(db_config, faker, teacher_count, subject_ids, school_id_to_data, school_count, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments]
        results = pool.starmap(create_teachers, arguments)
    return results


def main():
    faker = Faker("ru_RU")
    teacher_count = int(os.getenv('TEACHER_COUNT'))
    school_count = int(os.getenv('SCHOOL_COUNT'))
    number_of_variants_per_subject = int(os.getenv('NUMBER_OF_VARIANTS_PER_SUBJECT'))
    number_of_variants_per_task = int(os.getenv('NUMBER_OF_VARIANTS_PER_TASK'))

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
            subject_ids, subject_id_to_name = create_subjects(cur, subjects_points)
            variant_id_to_subject_id = create_variants(cur, subject_ids, number_of_variants_per_subject)
            task_ids, task_id_to_task_data = create_tasks(cur, subject_id_to_name, subjects_points, number_of_variants_per_task)
            create_links_between_tasks_and_variants(
                cur, task_id_to_task_data, variant_id_to_subject_id, subject_id_to_name, subjects_points
            )

            school_id_to_data = create_schools(cur, faker, school_count, address_ids)

    teacher_ids = create_teachers_parallel(db_config, faker, teacher_count, subject_ids, school_id_to_data, school_count, num_processes=5)


    return 0


if __name__ == "__main__":
    main()

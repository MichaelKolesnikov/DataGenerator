import copy
import os
import random
import json
from multiprocessing import Pool, Manager
from truncate import truncate

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

def concatenate_subarrays(arrays):
    result = []
    for subarray in arrays:
        result.extend(subarray)
    return result

# Schools
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
            faker.country(),
        )
        final_data.append(address)
        address_id += 1

    cur.executemany(
        "INSERT INTO Address (id, street, city, state, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s);",
        final_data,
    )
@measure_time
def create_schools(cur, school_count):
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

# For subjects
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


def create_exams(db_config, faker: Faker, school_ids, subject_ids, start, end):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            exam_data = []
            exam_id = len(subject_ids) * start + 1

            for i in range(start, end):
                for subject_id in subject_ids:
                    exam_data.append(
                        (
                            exam_id,
                            faker.date_time_between(start_date="-1y", end_date="now"),
                            school_ids[i],
                            subject_id
                        )
                    )
                    exam_id += 1

            cur.executemany(
                "insert into exam (id, timedate, school_id, subject_id) values (%s, %s, %s, %s);",
                exam_data,
            )
    return exam_data
@measure_time
def create_exams_parallel(db_config, faker: Faker, school_ids, subject_ids, school_count, num_processes):
    with Pool(processes=num_processes) as pool:
        chunk_size = school_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        arguments = ((db_config, faker, school_ids, subject_ids, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
        result = pool.starmap(create_exams, arguments)
    return concatenate_subarrays(result)

def create_teachers(db_config, faker: Faker, subject_ids, school_ids, school_id_to_data, city_to_exam, address_id_first, start, end):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            teachers_data = []
            addresses_data = []
            teacher_with_exam_data = []

            address_id = address_id_first + start
            teacher_id = start + 1
            for teacher_number in range(start, end):
                school_id = random.choice(school_ids)
                city, state = school_id_to_data[school_id]
                subject_id = random.choice(subject_ids)
                addresses_data.append(
                    (
                        address_id,
                        faker.street_name(),
                        city,
                        state,
                        faker.postcode(),
                        faker.country(),
                    )
                )
                teachers_data.append(
                    [
                        teacher_id,
                        faker.name(),
                        address_id,
                        school_id,
                        subject_id,
                    ]
                )

                for ex_id, sub_id in random.sample(city_to_exam[city], 10):
                    if sub_id == subject_id:
                        continue
                    teacher_with_exam_data.append(
                        (
                            ex_id,
                            teacher_id
                        )
                    )

                address_id += 1
                teacher_id += 1
                if teacher_number % 10000 == 0:
                    print(f"Teacher number: {teacher_number - start}")
                    cur.executemany(
                        "INSERT INTO Address (id, street, city, state, postal_code, country) values (%s, %s, %s, %s, %s, %s);",
                        addresses_data,
                    )
                    cur.executemany(
                        """INSERT INTO Teacher (id, FullName, address_id, school_id, subject_id) VALUES (%s, %s, %s, %s, %s);""",
                        teachers_data,
                    )
                    cur.executemany(
                        "insert into teacherwithexam (exam_id, teacher_id) values (%s, %s);",
                        teacher_with_exam_data,
                    )
                    addresses_data.clear()
                    teachers_data.clear()
                    teacher_with_exam_data.clear()
            if addresses_data:
                cur.executemany(
                    "INSERT INTO Address (id, street, city, state, postal_code, country) values (%s, %s, %s, %s, %s, %s);",
                    addresses_data,
                )
                cur.executemany(
                    """INSERT INTO Teacher (id, FullName, address_id, school_id, subject_id) VALUES (%s, %s, %s, %s, %s);""",
                    teachers_data,
                )
                cur.executemany(
                    "insert into teacherwithexam (exam_id, teacher_id) values (%s, %s);",
                    teacher_with_exam_data,
                )
                addresses_data.clear()
                teachers_data.clear()
                teacher_with_exam_data.clear()
@measure_time
def create_teachers_parallel(db_config, faker: Faker, teacher_count, subject_ids, school_id_to_data, city_to_exam, address_id_first, num_processes):
    with Pool(processes=num_processes) as pool:
        chunk_size = teacher_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        school_ids = list(school_id_to_data.keys())
        arguments = ((db_config, faker, subject_ids, school_ids, school_id_to_data, city_to_exam, address_id_first, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
        pool.starmap(create_teachers, arguments)
    return list(range(1, teacher_count + 1))

def create_school_children(db_config, faker: Faker, school_ids, school_id_to_data, address_id_first, start, end):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            school_children_data = []
            addresses_data = []
            address_id = address_id_first + start
            school_children_id = start + 1

            for school_child_number in range(start, end):
                school_id = random.choice(school_ids)
                addresses_data.append(
                    (
                        address_id,
                        faker.street_name(),
                        school_id_to_data[school_id][0],
                        school_id_to_data[school_id][1],
                        faker.postcode(),
                        faker.country(),
                    )
                )
                s = str(school_children_id)
                school_children_data.append(
                    [
                        school_children_id,
                        faker.name(),
                        (10 - len(s)) * '0' + s,
                        faker.date_of_birth(),
                        school_id,
                        address_id,
                    ]
                )
                address_id += 1
                school_children_id += 1
                if school_child_number % 10000 == 0:
                    print(f"Schoolchild number: {school_child_number - start}")
                    cur.executemany(
                        "INSERT INTO Address (id, street, city, state, postal_code, country) values (%s, %s, %s, %s, %s, %s);",
                        addresses_data,
                    )
                    cur.executemany(
                        """INSERT INTO Schoolchild (id, FullName, passport_series_number, birthday, school_id, address_id) VALUES (%s, %s, %s, %s, %s, %s);""",
                        school_children_data,
                    )
                    addresses_data.clear()
                    school_children_data.clear()
            if addresses_data:
                cur.executemany(
                    "INSERT INTO Address (id, street, city, state, postal_code, country) values (%s, %s, %s, %s, %s, %s);",
                    addresses_data,
                )
                cur.executemany(
                    """INSERT INTO Schoolchild (id, FullName, passport_series_number, birthday, school_id, address_id) VALUES (%s, %s, %s, %s, %s, %s);""",
                    school_children_data,
                )
                addresses_data.clear()
                school_children_data.clear()
@measure_time
def create_school_children_parallel(db_config, faker: Faker, school_children_count, school_id_to_data, address_id_first, num_processes):
    with Pool(processes=num_processes) as pool:
        chunk_size = school_children_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        school_ids = list(school_id_to_data.keys())
        arguments = ((db_config, faker, school_ids, school_id_to_data, address_id_first, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
        pool.starmap(create_school_children, arguments)
    return list(range(1, school_children_count + 1))



def main():
    truncate()

    faker = Faker("ru_RU")
    teacher_count = int(os.getenv('TEACHER_COUNT'))
    school_count = int(os.getenv('SCHOOL_COUNT'))
    number_of_variants_per_subject = int(os.getenv('NUMBER_OF_VARIANTS_PER_SUBJECT'))
    number_of_variants_per_task = int(os.getenv('NUMBER_OF_VARIANTS_PER_TASK'))
    school_children_count = int(os.getenv('SCHOOLCHILDREN_COUNT'))

    db_config = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            create_addresses_for_schools(cur, faker, school_count)
            school_id_to_data = create_schools(cur, school_count)

            subjects_points = parse_subject_mapping('subject_data.txt')
            subject_ids, subject_id_to_name = create_subjects(cur, subjects_points)
            variant_id_to_subject_id = create_variants(cur, subject_ids, number_of_variants_per_subject)
            task_ids, task_id_to_task_data = create_tasks(cur, subject_id_to_name, subjects_points, number_of_variants_per_task)
            create_links_between_tasks_and_variants(cur, task_id_to_task_data, variant_id_to_subject_id, subject_id_to_name, subjects_points)

    exam_data = create_exams_parallel(db_config, faker, list(school_id_to_data.keys()), subject_ids, school_count, num_processes=5)
    city_to_exam = {}
    for exam_id, _, school_id, subject_id in exam_data:
        city_ = school_id_to_data[school_id][0]
        if city_ not in city_to_exam:
            city_to_exam[city_] = []
        city_to_exam[city_].append([exam_id, subject_id])
    teacher_ids = create_teachers_parallel(db_config, faker, teacher_count, subject_ids, school_id_to_data, city_to_exam, school_count + 1, num_processes=5)
    school_children_ids = create_school_children_parallel(db_config, faker, school_children_count, school_id_to_data, school_count + teacher_count + 1, num_processes=5)

    return 0


if __name__ == "__main__":
    main()

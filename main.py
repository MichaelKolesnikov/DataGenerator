import os
import random
from multiprocessing import Pool
from truncate import truncate

from dotenv import load_dotenv
load_dotenv()

from faker import Faker
import psycopg2
from functions import measure_time, concatenate_subarrays

from create_subjects import parse_subject_mapping, create_subjects, create_variants, create_tasks, create_links_between_tasks_and_variants
from create_schools import create_schools

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

def create_school_children(
        db_config, faker: Faker,
        school_ids, school_id_to_data, city_to_exam, subject_name_to_id, subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, address_id_first, start, end
):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            school_children_data = []
            addresses_data = []
            schoolchild_with_exam_data = []
            written_work_data = []
            written_task_data = []

            address_id = address_id_first + start
            school_children_id = start + 1

            for school_child_number in range(start, end):
                school_id = random.choice(school_ids)
                city, state = school_id_to_data[school_id]
                addresses_data.append([
                    address_id,
                    faker.street_name(),
                    city,
                    state,
                    faker.postcode(),
                    faker.country(),
                ])
                s = str(school_children_id)
                school_children_data.append([
                    school_children_id,
                    faker.name(),
                    (10 - len(s)) * '0' + s,
                    faker.date_of_birth(),
                    school_id,
                    address_id,
                ])

                exams = city_to_exam[city]
                rus_id = subject_name_to_id['Русский язык']
                basic_math_id = subject_name_to_id["Математика. Базовый уровень"]
                prof_math_id = subject_name_to_id["Математика. Профильный уровень"]
                rus_exam = random.choice(list(filter(lambda x: x[1] == rus_id, exams)))[0]
                math_exam, math_sub_id = random.choice(list(filter(lambda x: x[1] in (basic_math_id, prof_math_id), exams)))
                third_exam, third_sub_id = random.choice(list(filter(lambda x: x[1] not in (rus_id, basic_math_id, prof_math_id), exams)))
                for ex_id in rus_exam, math_exam, third_exam:
                    schoolchild_with_exam_data.append(
                        (
                            school_children_id,
                            ex_id
                        )
                    )
                written_work_id = 3 * school_children_id
                for sub_id in rus_id, math_sub_id, third_sub_id:
                    variant_id = random.choice(subject_id_to_variant_ids[sub_id])
                    written_work_data.append([
                        written_work_id,
                        school_children_id,
                        variant_id
                    ])
                    for task_number in range(1, len(subject_id_to_tasks[sub_id]) + 1):
                        task_id = random.choice(task_number_subject_to_ids[task_number, sub_id])
                        written_task_data.append([
                            random.randint(0, len(subject_id_to_tasks[sub_id][task_number])),
                            written_work_id,
                            random.choice(subject_id_to_teacher_ids[sub_id]),
                            task_id
                        ])

                    written_work_id -= 1

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
                    cur.executemany(
                        "insert into schoolchildwithexam (school_child_id, exam_id) values (%s, %s);",
                        schoolchild_with_exam_data,
                    )
                    cur.executemany(
                        "insert into writtenwork (id, school_child_id, variant_id) values (%s, %s, %s);",
                        written_work_data,
                    )
                    cur.executemany(
                        "insert into writtentask (grade, written_work_id, teacher_id, task_id) values (%s, %s, %s, %s);",
                        written_task_data,
                    )
                    addresses_data.clear()
                    school_children_data.clear()
                    schoolchild_with_exam_data.clear()
                    written_work_data.clear()
                    written_task_data.clear()
            if addresses_data:
                cur.executemany(
                    "INSERT INTO Address (id, street, city, state, postal_code, country) values (%s, %s, %s, %s, %s, %s);",
                    addresses_data,
                )
                cur.executemany(
                    """INSERT INTO Schoolchild (id, FullName, passport_series_number, birthday, school_id, address_id) VALUES (%s, %s, %s, %s, %s, %s);""",
                    school_children_data,
                )
                cur.executemany(
                    "insert into schoolchildwithexam (school_child_id, exam_id) values (%s, %s);",
                    schoolchild_with_exam_data,
                )
                cur.executemany(
                    "insert into writtenwork (id, school_child_id, variant_id) values (%s, %s, %s);",
                    written_work_data,
                )
                cur.executemany(
                    "insert into writtentask (grade, written_work_id, teacher_id, task_id) values (%s, %s, %s, %s);",
                    written_task_data,
                )
                addresses_data.clear()
                school_children_data.clear()
                schoolchild_with_exam_data.clear()
                written_work_data.clear()
                written_task_data.clear()
@measure_time
def create_school_children_parallel(
        db_config, faker: Faker,
        school_children_count, school_id_to_data, city_to_exam, subject_name_to_id, subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, address_id_first, num_processes
):
    with Pool(processes=num_processes) as pool:
        chunk_size = school_children_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        school_ids = list(school_id_to_data.keys())
        arguments = ((db_config, faker, school_ids, school_id_to_data, city_to_exam, subject_name_to_id, subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, address_id_first, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
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
            school_id_to_data = create_schools(cur, faker, school_count)

            subjects_points = parse_subject_mapping('subject_data.txt')
            subject_ids, subject_id_to_name, subject_name_to_id, subject_id_to_tasks = create_subjects(cur, subjects_points)
            variant_id_to_subject_id = create_variants(cur, subject_ids, number_of_variants_per_subject)
            subject_id_to_variant_ids = {}
            for var_id in variant_id_to_subject_id:
                sub_id = variant_id_to_subject_id[var_id]
                if sub_id not in subject_id_to_variant_ids:
                    subject_id_to_variant_ids[sub_id] = []
                subject_id_to_variant_ids[sub_id].append(var_id)
            task_ids, task_id_to_task_data, task_number_subject_to_ids = create_tasks(cur, subject_id_to_name, subjects_points, number_of_variants_per_task)
            create_links_between_tasks_and_variants(cur, task_id_to_task_data, variant_id_to_subject_id, subject_id_to_name, subjects_points)

    exam_data = create_exams_parallel(db_config, faker, list(school_id_to_data.keys()), subject_ids, school_count, num_processes=5)
    city_to_exam = {}
    for exam_id, _, school_id, subject_id in exam_data:
        city_ = school_id_to_data[school_id][0]
        if city_ not in city_to_exam:
            city_to_exam[city_] = []
        city_to_exam[city_].append([exam_id, subject_id])
    teacher_ids = create_teachers_parallel(db_config, faker, teacher_count, subject_ids, school_id_to_data, city_to_exam, school_count + 1, num_processes=5)
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("select id, subject_id from teacher;")
            subject_id_to_teacher_ids = {}
            for row in cur.fetchall():
                t_id, s_id = row
                if s_id not in subject_id_to_teacher_ids:
                    subject_id_to_teacher_ids[s_id] = []
                subject_id_to_teacher_ids[s_id].append(t_id)

    school_children_ids = create_school_children_parallel(
        db_config, faker,
        school_children_count, school_id_to_data, city_to_exam, subject_name_to_id, subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, school_count + teacher_count + 1, num_processes=5
    )

    return 0


if __name__ == "__main__":
    main()

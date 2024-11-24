from functions import measure_time
from faker import Faker
import psycopg2
import random
from multiprocessing import Pool


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

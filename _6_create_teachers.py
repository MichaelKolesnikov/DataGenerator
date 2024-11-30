from functions import measure_time
from faker import Faker
import psycopg2
import random
from multiprocessing import Pool
import pickle
from _3_create_schools import get_cities_states_info


def create_teachers(db_config, faker: Faker,
                    subject_count, city_to_exam, city_id_to_state_id,
                    school_count, start, end):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            teachers_data = []
            addresses_data = []
            teacher_with_exam_data = []

            def write_data():
                cur.executemany(
                    "INSERT INTO Address (id, street, city_id, state_id, postal_code, country_id) values (%s, %s, %s, %s, %s, %s);",
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

            address_id = school_count + start
            teacher_id = start
            for teacher_number in range(start, end):
                school_id = random.randint(0, school_count-1)
                city_id = school_id % len(city_id_to_state_id)
                subject_id = random.randint(0, subject_count-1)
                addresses_data.append(
                    (
                        address_id,
                        faker.street_name(),
                        city_id,
                        city_id_to_state_id[city_id],
                        faker.postcode(),
                        0,
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

                for ex_id, sub_id in random.sample(city_to_exam[city_id], 10):
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
                    write_data()
            if addresses_data:
                write_data()


@measure_time
def create_teachers_parallel(db_config, faker: Faker, teacher_count, subject_count, city_to_exam, city_id_to_state_id, school_count, num_processes):
    with Pool(processes=num_processes) as pool:
        chunk_size = teacher_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        arguments = ((db_config, faker, subject_count, city_to_exam, city_id_to_state_id, school_count, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
        pool.starmap(create_teachers, arguments)
    return list(range(1, teacher_count + 1))


def main():
    from config import db_config, teacher_count, SUBJECT_COUNT, school_count
    faker = Faker("ru_RU")
    with open('city_to_exam.pkl', 'rb') as f:
        city_to_exam = pickle.load(f)
    city_id_to_name, city_id_to_state_id, state_id_to_name = get_cities_states_info()
    teacher_ids = create_teachers_parallel(db_config, faker, teacher_count, SUBJECT_COUNT, city_to_exam, city_id_to_state_id, school_count, num_processes=5)
    pass


if __name__ == "__main__":
    main()

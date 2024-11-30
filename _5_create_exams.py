from functions import measure_time, concatenate_subarrays
from faker import Faker
import psycopg2
from multiprocessing import Pool
import pickle


def create_exams(db_config, faker: Faker, subject_count, start, end):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            exam_data = []
            exam_id = subject_count * start

            for i in range(start, end):
                for subject_id in range(subject_count):
                    exam_data.append(
                        (
                            exam_id,
                            faker.date_time_between(start_date="-1y", end_date="now"),
                            i,
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
def create_exams_parallel(db_config, faker: Faker, subject_ids, school_count, num_processes):
    with Pool(processes=num_processes) as pool:
        chunk_size = school_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        arguments = ((db_config, faker, subject_ids, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
        result = pool.starmap(create_exams, arguments)
    return concatenate_subarrays(result)


def main():
    from config import db_config, SUBJECT_COUNT, school_count
    from _3_create_schools import get_cities_states_info
    city_id_to_name, city_id_to_state_id, state_id_to_name = get_cities_states_info()

    faker = Faker("ru_RU")
    exam_data = create_exams_parallel(db_config, faker, SUBJECT_COUNT, school_count, num_processes=5)
    city_to_exam = {}
    for exam_id, _, school_id, subject_id in exam_data:
        city_ = school_id % len(city_id_to_name)
        if city_ not in city_to_exam:
            city_to_exam[city_] = []
        city_to_exam[city_].append([exam_id, subject_id])
    with open('city_to_exam.pkl', 'wb') as f:
        pickle.dump(city_to_exam, f)


if __name__ == "__main__":
    main()

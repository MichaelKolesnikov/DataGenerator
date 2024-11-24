from functions import measure_time, concatenate_subarrays
from faker import Faker
import psycopg2
from multiprocessing import Pool


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

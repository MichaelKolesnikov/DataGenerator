from functions import measure_time
from faker import Faker
import random
import psycopg2
from multiprocessing import Pool


def create_school_children(
        db_config, faker: Faker,
        school_ids, school_id_to_data,
        rus_id, city_to_rus_exams, city_to_math_exams, city_to_prof_exams,
        subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, address_id_first, start, end
):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            school_children_data = []
            addresses_data = []
            schoolchild_with_exam_data = []
            written_work_data = []
            written_task_data = []

            def write_data():
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

                rus_exam = random.choice(city_to_rus_exams[city])[0]
                math_exam, math_sub_id = random.choice(city_to_math_exams[city])
                third_exam, third_sub_id = random.choice(city_to_prof_exams[city])
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
                    write_data()
            if addresses_data:
                write_data()


@measure_time
def create_school_children_parallel(
        db_config, faker: Faker,
        school_children_count, school_id_to_data,
        rus_id, city_to_rus_exams, city_to_math_exams, city_to_prof_exams,
        subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, address_id_first, num_processes
):
    with Pool(processes=num_processes) as pool:
        chunk_size = school_children_count // num_processes
        variable_arguments = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
        school_ids = list(school_id_to_data.keys())
        arguments = ((db_config, faker, school_ids, school_id_to_data,
                      rus_id, city_to_rus_exams, city_to_math_exams, city_to_prof_exams,
                      subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, address_id_first, var_arg1, var_arg2) for var_arg1, var_arg2 in variable_arguments)
        pool.starmap(create_school_children, arguments)
    return list(range(1, school_children_count + 1))

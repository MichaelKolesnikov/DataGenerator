import os
from truncate import truncate

from dotenv import load_dotenv
load_dotenv()

from faker import Faker
import psycopg2

from create_subjects import parse_subject_mapping, create_subjects, create_variants, create_tasks, create_links_between_tasks_and_variants
from create_schools import create_schools
from create_exams import create_exams_parallel
from create_teachers import create_teachers_parallel
from create_school_children import create_school_children_parallel


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

    rus_id = subject_name_to_id['Русский язык']
    basic_math_id = subject_name_to_id["Математика. Базовый уровень"]
    prof_math_id = subject_name_to_id["Математика. Профильный уровень"]
    city_to_rus_exams = {}
    city_to_math_exams = {}
    city_to_prof_exams = {}
    for school_id in school_id_to_data:
        city = school_id_to_data[school_id][0]
        exams = city_to_exam[city]
        if city not in city_to_rus_exams:
            city_to_rus_exams[city] = list(filter(lambda x: x[1] == rus_id, exams))
            city_to_math_exams[city] = list(filter(lambda x: x[1] in (basic_math_id, prof_math_id), exams))
            city_to_prof_exams[city] = list(filter(lambda x: x[1] not in (rus_id, basic_math_id, prof_math_id), exams))

    school_children_ids = create_school_children_parallel(
        db_config, faker,
        school_children_count, school_id_to_data, rus_id, city_to_rus_exams, city_to_math_exams, city_to_prof_exams,
        subject_id_to_variant_ids, task_number_subject_to_ids, subject_id_to_tasks, subject_id_to_teacher_ids, school_count + teacher_count + 1, num_processes=5
    )

    return 0


if __name__ == "__main__":
    main()

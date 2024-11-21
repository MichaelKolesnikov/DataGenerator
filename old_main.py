import random
from faker import Faker
import psycopg2
import json

faker = Faker("ru_RU")

DB_CONFIG = {
    'dbname': 'test_db',  # Имя базы данных (по умолчанию 'postgres')
    'user': 'root',    # Имя пользователя (по умолчанию 'postgres')
    'password': 'root',  # Пароль, который вы установили
    'host': 'localhost',   # Хост, на котором запущен PostgreSQL (localhost, если контейнер на локальной машине)
    'port': '54320',
}




def create_students(cur, addresses, school_ids):
    student_ids = []
    for _ in range(NUM_SCHOOLCHILDREN):
        student = (
            faker.name(),
            faker.bothify(text="#### ######"),
            faker.date_of_birth(minimum_age=17, maximum_age=19),
            random.choice(school_ids),
            addresses.pop(0),
        )
        cur.execute(
            "INSERT INTO SchoolChild (FullName, passport_series_number, birthday, school_id, address_id) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
            student,
        )
        student_ids.append(cur.fetchone()[0])
    return student_ids


def create_exams(cur, school_ids, subject_ids):
    exam_ids = []
    for _ in range(len(subject_ids) * 10):  # Примерное количество экзаменов
        exam = (
            faker.date_time_between(start_date="-1y", end_date="now"),
            random.choice(school_ids),
            random.choice(subject_ids),
        )
        cur.execute(
            "INSERT INTO Exam (TimeDate, school_id, subject_id) VALUES (%s, %s, %s) RETURNING id;",
            exam,
        )
        exam_ids.append(cur.fetchone()[0])
    return exam_ids


def create_schoolchildwithexam(cur, student_ids, exam_ids, subject_ids):
    required_subjects = ["Русский язык", "Математика (базовая)", "Математика (профильная)"]
    optional_subjects = set(SUBJECTS) - set(required_subjects)

    subject_to_id = {name: sid for name, sid in zip(SUBJECTS, subject_ids)}
    russian_id = subject_to_id["Русский язык"]
    math_basic_id = subject_to_id["Математика (базовая)"]
    math_profile_id = subject_to_id["Математика (профильная)"]

    final_data = []
    russian_exam_ids = [eid for eid in exam_ids if cur.execute("SELECT subject_id FROM Exam WHERE id = %s;", (eid,)) or cur.fetchone()[0] == russian_id]
    basic_math_exam_ids = [
            eid for eid in exam_ids if cur.execute(
                "SELECT subject_id FROM Exam WHERE id = %s;", (eid,)
            ) or cur.fetchone()[0] == math_basic_id
        ]
    profile_math_exam_ids = [
        eid for eid in exam_ids if cur.execute(
            "SELECT subject_id FROM Exam WHERE id = %s;", (eid,)
        ) or cur.fetchone()[0] == math_profile_id
    ]
    optional_exam_ids = [
        eid for eid in exam_ids if cur.execute(
            "SELECT subject_id FROM Exam WHERE id = %s;", (eid,)
        ) or cur.fetchone()[0] not in [math_profile_id, math_basic_id, russian_id]
    ]
    for student_id in student_ids:
        russian_exam_id = random.choice(russian_exam_ids)
        final_data.append((student_id, russian_exam_id))

        math_exam_id = 0
        choice = random.choice([0, 1])
        if choice:
            math_exam_id = random.choice(profile_math_exam_ids)
        else:
            math_exam_id = random.choice(basic_math_exam_ids)
        final_data.append((student_id, math_exam_id))

        optional_exam_id = random.choice(optional_exam_ids)
        final_data.append((student_id, optional_exam_id))
    cur.executemany(
        "INSERT INTO SchoolChildWithExam (school_child_id, exam_id) VALUES (%s, %s) RETURNING id;",
        final_data,
    )
    cur.execute("SELECT id FROM schoolchildwithexam;")
    schoolchildwithexam_ids = [row[0] for row in cur.fetchall()]
    return schoolchildwithexam_ids




def create_tasks_and_links(cur, variant_ids):
    task_ids = []
    for variant_id in variant_ids:
        for i in range(TASKS_PER_VARIANT):
            task = (f"Описание задания {i+1}", random.randint(1, 5))
            cur.execute(
                "INSERT INTO Task (description, price) VALUES (%s, %s) RETURNING id;",
                task,
            )
            task_id = cur.fetchone()[0]
            task_ids.append(task_id)
            cur.execute(
                "INSERT INTO TaskWithVariant (task_id, variant_id) VALUES (%s, %s);",
                (task_id, variant_id),
            )
    return task_ids

def create_writtenworks(cur, student_ids, variant_ids):
    writtenwork_ids = []
    for student_id in student_ids[:len(variant_ids)]:  # Ограничиваем кол-во
        variant_id = random.choice(variant_ids)
        cur.execute(
            "INSERT INTO WrittenWork (school_child_id, variant_id) VALUES (%s, %s) RETURNING id;",
            (student_id, variant_id),
        )
        writtenwork_ids.append(cur.fetchone()[0])
    return writtenwork_ids

def create_writtentasks(cur, writtenwork_ids, task_ids, teacher_ids):
    for written_work_id in writtenwork_ids:
        for task_id in random.sample(task_ids, TASKS_PER_VARIANT):
            grade = random.randint(0, 100)
            teacher_id = random.choice(teacher_ids)
            cur.execute(
                "INSERT INTO WrittenTask (grade, written_work_id, teacher_id, task_id) VALUES (%s, %s, %s, %s);",
                (grade, written_work_id, teacher_id, task_id),
            )


def main():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # base tables
            subject_ids = create_subjects(cur)
            addresses = create_addresses(cur)
            school_ids = create_schools(cur, addresses)
            student_ids = create_students(cur, addresses, school_ids)
            teacher_ids = create_teachers(cur, addresses, school_ids, subject_ids)
            exam_ids = create_exams(cur, school_ids, subject_ids)
            variant_ids = create_variants(cur, subject_ids)
            task_ids = create_tasks_and_links(cur, variant_ids)

            # not base tables
            create_schoolchildwithexam(cur, student_ids, exam_ids, subject_ids)
            conn.commit()
            '''




            writtenwork_ids = create_writtenworks(cur, student_ids, variant_ids)
            create_writtentasks(cur, writtenwork_ids, task_ids, teacher_ids)
            '''

if __name__ == "__main__":
    main()

import copy
import json
from functions import measure_time
import psycopg2
import pickle


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
    subject_id_to_tasks = [0] * 17
    sub_id = 0
    subject_id_to_name = []
    for subject in subjects_points:
        score_mapping = subjects_points[subject]
        score_mapping_json = json.dumps(score_mapping, ensure_ascii=False)
        subjects_data.append((sub_id, subject, score_mapping_json))

        subject_id_to_tasks[sub_id] = subjects_points[subject]
        subject_id_to_name.append(subject)
        sub_id += 1
    cur.executemany(
        "INSERT INTO Subject (id, name, ScoreMapping_JSON) VALUES (%s, %s, %s);",
        subjects_data,
    )

    cur.execute("SELECT id, name FROM Subject;")
    subject_id_to_name = {row[0]: row[1] for row in cur.fetchall()}
    subject_name_to_id = {subject_id_to_name[i]: i for i in subject_id_to_name}
    return subject_id_to_name, subject_name_to_id, subject_id_to_tasks


@measure_time
def create_variants(cur, number_of_variants_per_subject, subject_count):
    variant_data = []
    variant_id = 0
    variant_id_to_subject_id = []
    for subject_id in range(subject_count):
        for i in range(number_of_variants_per_subject):
            variant_data.append(
                (variant_id, (i + 1), subject_id)
            )
            variant_id_to_subject_id.append(subject_id)
            variant_id += 1
    cur.executemany(
        "INSERT INTO Variant (id, number, subject_id) VALUES (%s, %s, %s);",
        variant_data,
    )
    return variant_id_to_subject_id


@measure_time
def create_tasks(cur, subject_id_to_name, subjects_points, number_of_variants_per_task: int):
    tasks_data = []
    task_id = 0
    task_id_to_task_data = []
    task_number_subject_to_ids = {}
    for subject_id in subject_id_to_name:
        subject_name = subject_id_to_name[subject_id]
        for task_number in subjects_points[subject_name]:
            for _ in range(number_of_variants_per_task):
                price = subjects_points[subject_name][task_number]
                description = f"{subject_name}-task"
                tasks_data.append(
                    (task_id, task_number, description, price)
                )
                task_id_to_task_data.append([task_number, subject_name])

                if (task_number, subject_id) not in task_number_subject_to_ids:
                    task_number_subject_to_ids[task_number, subject_id] = []
                task_number_subject_to_ids[task_number, subject_id].append(task_id)
                task_id += 1
    cur.executemany(
        "insert into task (id, number, description, price) values (%s, %s, %s, %s);",
        tasks_data,
    )
    assert task_id == len(task_id_to_task_data)
    return task_id_to_task_data, task_number_subject_to_ids


@measure_time
def create_links_between_tasks_and_variants(cur, task_id_to_task_data_, variant_id_to_subject_id, subject_id_to_name):
    task_id_to_task_data = copy.deepcopy(task_id_to_task_data_)
    variant_task = {}
    link_data = []
    for variant_id in variant_id_to_subject_id:
        subject_id = variant_id_to_subject_id[variant_id]
        subject_name = subject_id_to_name[subject_id]
        to_remove = []
        for task_id in range(len(task_id_to_task_data)):
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


def main():
    from config import db_config, number_of_variants_per_subject, number_of_variants_per_task, SUBJECT_COUNT

    subjects_points = parse_subject_mapping("subject_data.txt")

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            subject_id_to_name, subject_name_to_id, subject_id_to_tasks = create_subjects(cur, subjects_points)
            rus_id = subject_name_to_id['Русский язык']
            basic_math_id = subject_name_to_id["Математика. Базовый уровень"]
            prof_math_id = subject_name_to_id["Математика. Профильный уровень"]

            variant_id_to_subject_id = create_variants(cur, number_of_variants_per_subject, SUBJECT_COUNT)
            subject_id_to_variant_ids = {}
            for var_id in range(len(variant_id_to_subject_id)):
                sub_id = variant_id_to_subject_id[var_id]
                if sub_id not in subject_id_to_variant_ids:
                    subject_id_to_variant_ids[sub_id] = []
                subject_id_to_variant_ids[sub_id].append(var_id)

            task_id_to_task_data, task_number_subject_to_ids = create_tasks(cur, subject_id_to_name, subjects_points, number_of_variants_per_task)

            create_links_between_tasks_and_variants(cur, task_id_to_task_data, variant_id_to_subject_id, subject_id_to_name)
            with open('subject_id_to_tasks.pkl', 'wb') as f:
                pickle.dump(subject_id_to_tasks, f)
            with open('subject_id_to_variant_ids.pkl', 'wb') as f:
                pickle.dump(subject_id_to_variant_ids, f)
            with open('task_id_to_task_data.pkl', 'wb') as f:
                pickle.dump(task_id_to_task_data, f)
            with open('task_number_subject_to_ids.pkl', 'wb') as f:
                pickle.dump(task_number_subject_to_ids, f)

            with open("sub.txt", 'w') as f:
                f.write(f"{rus_id} {basic_math_id} {prof_math_id}\n")




if __name__ == "__main__":
    main()

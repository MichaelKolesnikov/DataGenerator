import copy
import json
from functions import measure_time


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
    sub_id = 1
    for subject in subjects_points:
        score_mapping = subjects_points[subject]
        score_mapping_json = json.dumps(score_mapping, ensure_ascii=False)
        subjects_data.append((sub_id, subject, score_mapping_json))
        subject_id_to_tasks[sub_id] = subjects_points[subject]
        sub_id += 1
    cur.executemany(
        "INSERT INTO Subject (id, name, ScoreMapping_JSON) VALUES (%s, %s, %s);",
        subjects_data,
    )

    cur.execute("SELECT id, name FROM Subject;")
    subject_id_to_name = {row[0]: row[1] for row in cur.fetchall()}
    subject_name_to_id = {subject_id_to_name[i]: i for i in subject_id_to_name}
    subject_ids = list(subject_id_to_name.keys())
    return subject_ids, subject_id_to_name, subject_name_to_id, subject_id_to_tasks


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
                task_id_to_task_data[task_id] = [task_number, subject_name]

                if (task_number, subject_id) not in task_number_subject_to_ids:
                    task_number_subject_to_ids[task_number, subject_id] = []
                task_number_subject_to_ids[task_number, subject_id].append(task_id)
                task_id += 1
    cur.executemany(
        "insert into task (id, number, description, price) values (%s, %s, %s, %s);",
        tasks_data,
    )
    task_ids = task_id_to_task_data.keys()
    return task_ids, task_id_to_task_data, task_number_subject_to_ids


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

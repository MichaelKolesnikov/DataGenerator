import os



#
#
# def create_schoolchildwithexam(cur, student_ids, exam_ids, subject_ids):
#     required_subjects = ["Русский язык", "Математика (базовая)", "Математика (профильная)"]
#     optional_subjects = set(SUBJECTS) - set(required_subjects)
#
#     subject_to_id = {name: sid for name, sid in zip(SUBJECTS, subject_ids)}
#     russian_id = subject_to_id["Русский язык"]
#     math_basic_id = subject_to_id["Математика (базовая)"]
#     math_profile_id = subject_to_id["Математика (профильная)"]
#
#     final_data = []
#     russian_exam_ids = [eid for eid in exam_ids if cur.execute("SELECT subject_id FROM Exam WHERE id = %s;", (eid,)) or cur.fetchone()[0] == russian_id]
#     basic_math_exam_ids = [
#             eid for eid in exam_ids if cur.execute(
#                 "SELECT subject_id FROM Exam WHERE id = %s;", (eid,)
#             ) or cur.fetchone()[0] == math_basic_id
#         ]
#     profile_math_exam_ids = [
#         eid for eid in exam_ids if cur.execute(
#             "SELECT subject_id FROM Exam WHERE id = %s;", (eid,)
#         ) or cur.fetchone()[0] == math_profile_id
#     ]
#     optional_exam_ids = [
#         eid for eid in exam_ids if cur.execute(
#             "SELECT subject_id FROM Exam WHERE id = %s;", (eid,)
#         ) or cur.fetchone()[0] not in [math_profile_id, math_basic_id, russian_id]
#     ]
#     for student_id in student_ids:
#         russian_exam_id = random.choice(russian_exam_ids)
#         final_data.append((student_id, russian_exam_id))
#
#         math_exam_id = 0
#         choice = random.choice([0, 1])
#         if choice:
#             math_exam_id = random.choice(profile_math_exam_ids)
#         else:
#             math_exam_id = random.choice(basic_math_exam_ids)
#         final_data.append((student_id, math_exam_id))
#
#         optional_exam_id = random.choice(optional_exam_ids)
#         final_data.append((student_id, optional_exam_id))
#     cur.executemany(
#         "INSERT INTO SchoolChildWithExam (school_child_id, exam_id) VALUES (%s, %s) RETURNING id;",
#         final_data,
#     )
#     cur.execute("SELECT id FROM schoolchildwithexam;")
#     schoolchildwithexam_ids = [row[0] for row in cur.fetchall()]
#     return schoolchildwithexam_ids
#
#
# def create_writtenworks(cur, student_ids, variant_ids):
#     writtenwork_ids = []
#     for student_id in student_ids[:len(variant_ids)]:  # Ограничиваем кол-во
#         variant_id = random.choice(variant_ids)
#         cur.execute(
#             "INSERT INTO WrittenWork (school_child_id, variant_id) VALUES (%s, %s) RETURNING id;",
#             (student_id, variant_id),
#         )
#         writtenwork_ids.append(cur.fetchone()[0])
#     return writtenwork_ids
#
# def create_writtentasks(cur, writtenwork_ids, task_ids, teacher_ids):
#     for written_work_id in writtenwork_ids:
#         for task_id in random.sample(task_ids, TASKS_PER_VARIANT):
#             grade = random.randint(0, 100)
#             teacher_id = random.choice(teacher_ids)
#             cur.execute(
#                 "INSERT INTO WrittenTask (grade, written_work_id, teacher_id, task_id) VALUES (%s, %s, %s, %s);",
#                 (grade, written_work_id, teacher_id, task_id),
#             )


def main():
    print(os.cpu_count())

if __name__ == "__main__":
    main()

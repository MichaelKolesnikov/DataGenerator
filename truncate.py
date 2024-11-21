import psycopg2


def main():
    db_config = {
        'dbname': 'test_db',
        'user': 'root',
        'password': 'root',
        'host': 'localhost',
        'port': '54320'
    }
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        tables = [
            "WrittenTask",
            "WrittenWork",
            "TaskWithVariant",
            "Task",
            "Variant",
            "TeacherWithExam",
            "Teacher",
            "SchoolChildWithExam",
            "Exam",
            "SchoolChild",
            "Subject",
            "School",
            "Address"
        ]

        truncate_queries = "; ".join([f"TRUNCATE TABLE {table} CASCADE" for table in tables])
        cur.execute(truncate_queries)
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

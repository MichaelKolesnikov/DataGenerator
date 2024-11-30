import psycopg2
from config import db_config

def truncate():
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        tables = [
            "City",
            "State",
            "Country",
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

def main():
    truncate()


if __name__ == "__main__":
    main()

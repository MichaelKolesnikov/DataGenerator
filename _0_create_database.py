import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
from functions import get_cities_states_info
from _4_create_subjects import parse_subject_mapping

teacher_count = int(os.getenv('TEACHER_COUNT'))
school_count = int(os.getenv('SCHOOL_COUNT'))
number_of_variants_per_subject = int(os.getenv('NUMBER_OF_VARIANTS_PER_SUBJECT'))
number_of_variants_per_task = int(os.getenv('NUMBER_OF_VARIANTS_PER_TASK'))
school_children_count = int(os.getenv('SCHOOLCHILDREN_COUNT'))
city_id_to_name, city_id_to_state_id, state_id_to_name = get_cities_states_info()
subjects_points = parse_subject_mapping('subject_data.txt')

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def main():
    try:
        with open('code.sql') as f:
            create_command = f.read()
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_command)

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
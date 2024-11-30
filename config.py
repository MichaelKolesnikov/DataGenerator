import os
from dotenv import load_dotenv
load_dotenv()

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

teacher_count = int(os.getenv('TEACHER_COUNT'))
school_count = int(os.getenv('SCHOOL_COUNT'))
number_of_variants_per_subject = int(os.getenv('NUMBER_OF_VARIANTS_PER_SUBJECT'))
number_of_variants_per_task = int(os.getenv('NUMBER_OF_VARIANTS_PER_TASK'))
school_children_count = int(os.getenv('SCHOOLCHILDREN_COUNT'))
SUBJECT_COUNT=16

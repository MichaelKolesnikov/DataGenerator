from truncate import truncate

import _2_create_cities_and_states
import _3_create_schools
import _4_create_subjects
import _5_create_exams
import _6_create_teachers
import _7_create_school_children


def main():
    truncate()
    _2_create_cities_and_states.main()
    _3_create_schools.main()
    _4_create_subjects.main()
    _5_create_exams.main()
    _6_create_teachers.main()
    _7_create_school_children.main()

    return 0


if __name__ == "__main__":
    main()

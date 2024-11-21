CREATE TABLE IF NOT EXISTS Address (
    id SERIAL PRIMARY KEY,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR
);

CREATE TABLE IF NOT EXISTS School (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    address_id INT,
    FOREIGN KEY (address_id) REFERENCES Address(id)
);

CREATE TABLE IF NOT EXISTS Subject (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    ScoreMapping_JSON JSON
);

CREATE TABLE IF NOT EXISTS SchoolChild (
    id SERIAL PRIMARY KEY,
    FullName VARCHAR,
    passport_series_number VARCHAR UNIQUE,
    birthday DATE,
    school_id INT,
    address_id INT,
    FOREIGN KEY (school_id) REFERENCES School(id),
    FOREIGN KEY (address_id) REFERENCES Address(id)
);

CREATE TABLE IF NOT EXISTS Exam (
    id SERIAL PRIMARY KEY,
    TimeDate TIMESTAMP,
    school_id INT,
    subject_id INT,
    FOREIGN KEY (school_id) REFERENCES School(id),
    FOREIGN KEY (subject_id) REFERENCES Subject(id)
);

CREATE TABLE IF NOT EXISTS SchoolChildWithExam (
    id SERIAL PRIMARY KEY,
    school_child_id INT,
    exam_id INT,
    FOREIGN KEY (school_child_id) REFERENCES SchoolChild(id),
    FOREIGN KEY (exam_id) REFERENCES Exam(id)
);

CREATE TABLE IF NOT EXISTS Teacher (
    id SERIAL PRIMARY KEY,
    FullName VARCHAR,
    address_id INT,
    school_id INT,
    subject_id INT,
    FOREIGN KEY (address_id) REFERENCES Address(id),
    FOREIGN KEY (school_id) REFERENCES School(id),
    FOREIGN KEY (subject_id) REFERENCES Subject(id)
);

CREATE TABLE IF NOT EXISTS TeacherWithExam (
    id SERIAL PRIMARY KEY,
    exam_id INT,
    teacher_id INT,
    FOREIGN KEY (exam_id) REFERENCES Exam(id),
    FOREIGN KEY (teacher_id) REFERENCES Teacher(id)
);

CREATE TABLE IF NOT EXISTS Variant (
    id SERIAL PRIMARY KEY,
    number INT CHECK (number >= 1),
    subject_id INT,
    FOREIGN KEY (subject_id) REFERENCES Subject(id)
);

CREATE TABLE IF NOT EXISTS Task (
    id SERIAL PRIMARY KEY,
    number INT CHECK (number >= 1),
    description VARCHAR,
    price INT CHECK (price >= 1)
);

CREATE TABLE IF NOT EXISTS TaskWithVariant (
    id SERIAL PRIMARY KEY,
    task_id INT,
    variant_id INT,
    FOREIGN KEY (task_id) REFERENCES Task(id),
    FOREIGN KEY (variant_id) REFERENCES Variant(id)
);

CREATE TABLE IF NOT EXISTS WrittenWork (
    id SERIAL PRIMARY KEY,
    school_child_id INT,
    variant_id INT,
    FOREIGN KEY (school_child_id) REFERENCES SchoolChild(id),
    FOREIGN KEY (variant_id) REFERENCES Variant(id)
);

CREATE TABLE IF NOT EXISTS WrittenTask (
    id SERIAL PRIMARY KEY,
    grade INT CHECK (grade >= 0 AND grade <= 100),
    written_work_id INT,
    teacher_id INT,
    task_id INT,
    FOREIGN KEY (written_work_id) REFERENCES WrittenWork(id),
    FOREIGN KEY (teacher_id) REFERENCES Teacher(id),
    FOREIGN KEY (task_id) REFERENCES Task(id)
);

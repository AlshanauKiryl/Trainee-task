CREATE TABLE rooms (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE students (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birthday DATE,
    sex CHAR(1),
    room_id INT,
    FOREIGN KEY (room_id) REFERENCES rooms(id)
);
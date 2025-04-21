CREATE DATABASE spark_demo;
USE spark_demo;
CREATE TABLE test_user (
                           id INT PRIMARY KEY,
                           name VARCHAR(100),
                           age INT
);

INSERT INTO test_user (id, name, age) VALUES
                                          (1, 'Alice', 25),
                                          (2, 'Bob', 30),
                                          (3, 'Charlie', 22),
                                          (4, 'Daniel', 32),
                                          (5, 'Edison', 18)
;

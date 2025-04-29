CREATE DATABASE spark_demo;
USE spark_demo;

CREATE TABLE test_user (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    region VARCHAR(50)  -- 添加分区列
);

INSERT INTO test_user (id, name, age, region) VALUES
    (1, 'Alice', 25, 'North'),
    (2, 'Bob', 30, 'South'),
    (3, 'Charlie', 22, 'East'),
    (4, 'Daniel', 32, 'West'),
    (5, 'Edison', 18, 'North');

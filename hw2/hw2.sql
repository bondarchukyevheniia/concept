CREATE DATABASE IF NOT EXISTS hw2_dw;
USE hw2_dw;

DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS calls;

CREATE TABLE employees (
  employee_id INT PRIMARY KEY AUTO_INCREMENT,
  full_name   VARCHAR(100) NOT NULL,
  team        VARCHAR(80)  NOT NULL,
  hire_date  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE calls (
  call_id    INT PRIMARY KEY AUTO_INCREMENT,
  employee_id  INT NOT NULL,
  call_time   DATETIME NOT NULL,
  phone  VARCHAR(20) NOT NULL,
  direction VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL,
  FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

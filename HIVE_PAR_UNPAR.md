# 🏫 University Departmental Performance Analysis using Apache Hive

## 📘 Overview

This project demonstrates how a university can analyze student performance data stored in CSV files on **HDFS** using **Hive**.
The analysis includes:

* Finding **average marks** per department
* Identifying **top-performing departments**
* Listing **students scoring above 90%**

It also compares query performance between **unpartitioned** and **partitioned** Hive tables.

---

## 🧩 Step 1: Create Database

```sql
CREATE DATABASE university_db;
USE university_db;
```

---

## 📁 Step 2: Prepare Sample Data

Save the following data as `student_marks.csv` and upload it to HDFS:

```
/user/hive/student_data/student_marks.csv
```

### Sample Content

```
1,Alice,CSE,95,6
2,Bob,ECE,78,6
3,Carol,CSE,88,6
4,David,MECH,92,6
5,Emma,CSE,97,6
6,Frank,ECE,90,6
7,Grace,MECH,84,6
8,Helen,EEE,91,6
```

---

## 🧮 Step 3: Create an Unpartitioned Table

```sql
CREATE TABLE student_unpartitioned (
    student_id INT,
    name STRING,
    department STRING,
    marks INT,
    semester INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/student_data/student_marks.csv' INTO TABLE student_unpartitioned;
```

---

## 🔍 Step 4: Run Queries on Unpartitioned Table

```sql
-- a) Average marks per department
SELECT department, AVG(marks) AS avg_marks
FROM student_unpartitioned
GROUP BY department;

-- b) Top-performing department
SELECT department, AVG(marks) AS avg_marks
FROM student_unpartitioned
GROUP BY department
ORDER BY avg_marks DESC
LIMIT 1;

-- c) Students scoring above 90%
SELECT name, department, marks
FROM student_unpartitioned
WHERE marks > 90;
```

---

## 🧱 Step 5: Create a Partitioned Table

```sql
CREATE TABLE student_partitioned (
    student_id INT,
    name STRING,
    marks INT,
    semester INT
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

---

## 📤 Step 6: Load Data into Partitioned Table

```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE student_partitioned PARTITION(department)
SELECT student_id, name, marks, semester, department
FROM student_unpartitioned;
```

---

## 🚀 Step 7: Run Queries on Partitioned Table

```sql
-- a) Average marks per department
SELECT department, AVG(marks) AS avg_marks
FROM student_partitioned
GROUP BY department;

-- b) Top-performing department
SELECT department, AVG(marks) AS avg_marks
FROM student_partitioned
GROUP BY department
ORDER BY avg_marks DESC
LIMIT 1;

-- c) Students scoring above 90% in CSE
SELECT name, marks
FROM student_partitioned
WHERE department = 'CSE' AND marks > 90;
```

---

## 📊 Step 8: Performance Comparison

| Query                | Table Type    | Data Scanned       | Example Time |
| -------------------- | ------------- | ------------------ | ------------ |
| Avg. Marks           | Unpartitioned | All rows           | 6.3 sec      |
| Avg. Marks           | Partitioned   | All partitions     | 5.8 sec      |
| >90% Students in CSE | Unpartitioned | All rows           | 6.1 sec      |
| >90% Students in CSE | Partitioned   | Only CSE partition | 2.3 sec      |

✅ Partitioned tables significantly reduce query time when filtering by the partition column (`department`).

---

## 🧾 Step 9: Verify Partitions

```sql
SHOW PARTITIONS student_partitioned;
```

**Example Output:**

```
department=CSE
department=ECE
department=EEE
department=MECH
```

---

## 🏁 Conclusion

Partitioning by department helps Hive **minimize data scanning**, thereby improving query performance for large datasets.
This approach scales efficiently as more departmental data is added over time.

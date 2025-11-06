# 💰 Finance Company Loan & Payment Analysis using Apache Hive

## 📘 Overview

This project demonstrates how a **finance company** can use **Apache Hive** to track customer **loan applications** and **payments**, identify **customers with delayed payments**, and compute **total outstanding loans per city**.
It also showcases how Hive performs **joins (inner and outer)** to handle missing or incomplete data.

---

## 🧩 Step 1: Create Database

```sql
CREATE DATABASE finance_db;
USE finance_db;
```

---

## 📁 Step 2: Prepare Sample Data

Save the following CSV files and upload them to HDFS.

### 🧾 Customer Data (`/user/hive/finance_data/customers.csv`)

```
1,Alice,Delhi,50000
2,Bob,Mumbai,30000
3,Charlie,Bangalore,45000
4,David,Delhi,60000
5,Emma,Chennai,40000
```

**Schema:**
(customer_id, name, city, loan_amount)

---

### 💸 Payment Data (`/user/hive/finance_data/payments.csv`)

```
1,2025-01-15,45000,OnTime
2,2025-01-25,25000,Delayed
3,2025-02-05,45000,OnTime
4,2025-02-20,40000,Delayed
6,2025-03-01,30000,Delayed
```

**Schema:**
(customer_id, payment_date, amount_paid, payment_status)

---

## 🧮 Step 3: Create Hive Tables

### a) Customer Table

```sql
CREATE TABLE customers (
    customer_id INT,
    name STRING,
    city STRING,
    loan_amount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/finance_data/customers.csv' INTO TABLE customers;
```

### b) Payment Table

```sql
CREATE TABLE payments (
    customer_id INT,
    payment_date STRING,
    amount_paid DOUBLE,
    payment_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/finance_data/payments.csv' INTO TABLE payments;
```

---

## 🔍 Step 4: Find Customers with Delayed Payments

```sql
SELECT c.customer_id, c.name, c.city, p.payment_date, p.payment_status
FROM customers c
JOIN payments p
ON c.customer_id = p.customer_id
WHERE p.payment_status = 'Delayed';
```

### ✅ Output Example

| customer_id | name  | city   | payment_date | payment_status |
| ----------- | ----- | ------ | ------------ | -------------- |
| 2           | Bob   | Mumbai | 2025-01-25   | Delayed        |
| 4           | David | Delhi  | 2025-02-20   | Delayed        |
| 6           | NULL  | NULL   | 2025-03-01   | Delayed        |

---

## 💰 Step 5: Calculate Total Outstanding Loans per City

```sql
SELECT c.city,
       SUM(c.loan_amount - IF(p.amount_paid IS NOT NULL, p.amount_paid, 0)) AS total_outstanding
FROM customers c
LEFT OUTER JOIN payments p
ON c.customer_id = p.customer_id
GROUP BY c.city;
```

### ✅ Output Example

| city      | total_outstanding |
| --------- | ----------------- |
| Delhi     | 25000             |
| Mumbai    | 5000              |
| Bangalore | 0                 |
| Chennai   | 40000             |

---

## 🔄 Step 6: Demonstrate Hive Outer Joins

### 🔸 LEFT OUTER JOIN (All customers, even if no payment record)

```sql
SELECT c.customer_id, c.name, p.payment_status
FROM customers c
LEFT OUTER JOIN payments p
ON c.customer_id = p.customer_id;
```

### 🔸 RIGHT OUTER JOIN (All payments, even if customer missing)

```sql
SELECT c.customer_id, c.name, p.payment_status
FROM customers c
RIGHT OUTER JOIN payments p
ON c.customer_id = p.customer_id;
```

### 🔸 FULL OUTER JOIN (All records from both tables)

```sql
SELECT c.customer_id, c.name, p.payment_status
FROM customers c
FULL OUTER JOIN payments p
ON c.customer_id = p.customer_id;
```

**Hive Behavior:**

* Missing data appears as `NULL` for unmatched records.
* Outer joins ensure no records are lost during the merge process.

---

## 📊 Step 7: Key Insights

| Query                      | Purpose                                  | Join Type       | Description                            |
| -------------------------- | ---------------------------------------- | --------------- | -------------------------------------- |
| Find delayed payments      | Identify customers with overdue payments | INNER JOIN      | Only customers with payment records    |
| Total outstanding per city | Calculate remaining loan balance         | LEFT OUTER JOIN | Includes customers with no payment yet |
| Missing data handling      | Validate unmatched records               | FULL OUTER JOIN | Displays NULL for missing data         |

---

## 🏁 Conclusion

This Hive warehouse helps finance teams:

* Detect **delayed payments** efficiently
* Monitor **loan performance per city**
* Handle **missing or unmatched data** gracefully using outer joins

Hive’s join mechanisms make it easy to integrate and analyze multi-table data stored across HDFS at scale.

--1. Create employee table with distribution key by key [ create new names ]
CREATE TABLE employee (
    name VARCHAR(50),
    address VARCHAR(100),
    salary INT,
    sex VARCHAR(10),
    o_date DATE,
    state VARCHAR(10)
)
DISTKEY(name);

INSERT INTO employee (name, address, salary, sex, o_date, state) VALUES
('john_doe', '123 Maple St', 50, 'male', '2023-05-01', 'NJ'),
('john_doe', '123 Maple St', 52, 'male', '2023-05-02', 'NJ'),
('john_doe', '123 Maple St', 55, 'male', '2023-05-03', 'NJ'),
('jane_smith', '456 Oak St', 57, 'female', '2023-05-02', 'NJ'),
('jane_smith', '456 Oak St', 59, 'female', '2023-05-03', 'NJ'),
('alice_jones', '789 Pine St', 60, 'female', '2023-05-01', 'NJ'),
('alice_jones', '789 Pine St', 64, 'female', '2023-05-03', 'NJ'),
('bob_brown', '101 Maple St', 65, 'male', '2023-05-01', 'NJ'),
('bob_brown', '101 Maple St', 67, 'male', '2023-05-02', 'NJ'),
('charlie_clark', '202 Oak St', 70, 'male', '2023-05-01', 'CA'),
('charlie_clark', '202 Oak St', 72, 'male', '2023-05-02', 'CA'),
('charlie_clark', '202 Oak St', 74, 'male', '2023-05-03', 'CA'),
('diana_evans', '303 Pine St', 75, 'female', '2023-05-01', 'NJ'),
('edward_harris', '404 Maple St', 84, 'male', '2023-05-03', 'CA'),
('frank_king', '505 Oak St', 85, 'male', '2023-05-01', 'NJ'),
('frank_king', '505 Oak St', 89, 'male', '2023-05-03', 'NJ'),
('george_lee', '606 Pine St', 90, 'male', '2023-05-01', 'NY'),
('george_lee', '606 Pine St', 94, 'male', '2023-05-03', 'NY'),
('hannah_martin', '707 Maple St', 95, 'female', '2023-05-01', 'NJ'),
('hannah_martin', '707 Maple St', 97, 'female', '2023-05-02', 'NJ'),
('hannah_martin', '707 Maple St', 99, 'female', '2023-05-03', 'NJ'),
('ian_nelson', '808 Oak St', 100, 'male', '2023-05-01', 'CA'),
('ian_nelson', '808 Oak St', 102, 'male', '2023-05-02', 'CA'),
('julia_ocean', '909 Pine St', 107, 'female', '2023-05-02', 'NJ'),
('kevin_price', '1010 Maple St', 112, 'male', '2023-05-02', 'NJ'),
('kevin_price', '1010 Maple St', 114, 'male', '2023-05-03', 'NJ');

--2. Create table state population table with dist key all.
CREATE TABLE state_population (
    state VARCHAR(10),
    population INT
)
DISTSTYLE ALL;

INSERT INTO state_population (state, population) VALUES
('NJ', 2000),
('NY', 300),
('CA', 500);

--3. Create both table with encoding enabled.

CREATE TABLE employee (
    name VARCHAR(50) ENCODE zstd,
    address VARCHAR(100) ENCODE zstd,
    salary INT ENCODE az64,
    sex VARCHAR(10) ENCODE zstd,
    o_date DATE ENCODE az64,
    state VARCHAR(10) ENCODE zstd
)
DISTKEY(name);

CREATE TABLE state_population (
    state VARCHAR(10) ENCODE zstd,
    population INT ENCODE az64
)
DISTSTYLE ALL;


--4. How  many employees in each day didnt get work ? (Here you have to join the tables)
WITH unique_dates AS (
    SELECT DISTINCT o_date
    FROM employee
),
unique_employees AS (
    SELECT DISTINCT name
    FROM employee
),
all_dates_employees AS (
    SELECT 
        d.o_date,
        e.name
    FROM 
        unique_dates d
    CROSS JOIN 
        unique_employees e
),
worked_dates AS (
    SELECT DISTINCT 
        name, 
        o_date 
    FROM 
        employee
),
employees_did_not_work AS (
    SELECT 
        a.o_date,
        a.name
    FROM 
        all_dates_employees a
    LEFT JOIN 
        worked_dates w
    ON 
        a.o_date = w.o_date 
        AND a.name = w.name
    WHERE 
        w.name IS NULL
),
employees_did_not_work_count AS (
    SELECT 
        o_date,
        COUNT(*) AS employees_did_not_work
    FROM 
        employees_did_not_work
    GROUP BY 
        o_date
)
SELECT 
    e.o_date,
    e.employees_did_not_work,
    sp.state,
    sp.population
FROM 
    employees_did_not_work_count e
JOIN 
    employee emp ON e.o_date = emp.o_date
JOIN 
    state_population sp ON emp.state = sp.state
GROUP BY 
    e.o_date, sp.state, sp.population, e.employees_did_not_work
ORDER BY 
    e.o_date;

--5. Do a explain on the above query. [ query 4]

XN Merge  (cost=1000005655047.18..1000005655047.24 rows=26 width=49)
  Merge Key: e.o_date
  ->  XN Network  (cost=1000005655047.18..1000005655047.24 rows=26 width=49)
        Send to leader
        ->  XN Sort  (cost=1000005655047.18..1000005655047.24 rows=26 width=49)
              Sort Key: e.o_date
              ->  XN HashAggregate  (cost=5655046.57..5655046.57 rows=26 width=49)
                    ->  XN Hash Join DS_DIST_ALL_NONE  (cost=4875044.88..5655046.31 rows=26 width=49)
                          Hash Cond: (("outer".state)::text = ("inner".state)::text)
                          ->  XN Hash Join DS_DIST_OUTER  (cost=4875044.28..5655045.12 rows=26 width=45)
                                Outer Dist Key: emp.o_date
                                Hash Cond: ("outer".o_date = "inner".o_date)
                                ->  XN Seq Scan on employee emp  (cost=0.00..0.26 rows=26 width=37)
                                ->  XN Hash  (cost=4875044.21..4875044.21 rows=26 width=12)
                                      ->  XN Subquery Scan e  (cost=4875043.89..4875044.21 rows=26 width=12)
                                            ->  XN HashAggregate  (cost=4875043.89..4875043.95 rows=26 width=4)
                                                  ->  XN Hash Left Join DS_DIST_INNER  (cost=0.78..4875040.50 rows=676 width=4)
                                                        Inner Dist Key: w.name
                                                        Hash Cond: ((("outer".name)::text = ("inner".name)::text) AND ("outer".o_date = "inner".o_date))
                                                        Filter: ("inner".name IS NULL)
                                                        ->  XN Nested Loop DS_BCAST_INNER  (cost=0.00..3120022.56 rows=676 width=97)
                                                              ->  XN Subquery Scan e  (cost=0.00..0.58 rows=26 width=93)
                                                                    ->  XN Unique  (cost=0.00..0.33 rows=26 width=93)
                                                                          ->  XN Seq Scan on employee  (cost=0.00..0.26 rows=26 width=93)
                                                              ->  XN Subquery Scan d  (cost=0.00..0.58 rows=26 width=4)
                                                                    ->  XN Unique  (cost=0.00..0.33 rows=26 width=4)
                                                                          ->  XN Seq Scan on employee  (cost=0.00..0.26 rows=26 width=4)
                                                        ->  XN Hash  (cost=0.65..0.65 rows=26 width=97)
                                                              ->  XN Subquery Scan w  (cost=0.00..0.65 rows=26 width=97)
                                                                    ->  XN Unique  (cost=0.00..0.39 rows=26 width=97)
                                                                          ->  XN Seq Scan on employee  (cost=0.00..0.26 rows=26 width=97)
                          ->  XN Hash  (cost=0.48..0.48 rows=48 width=37)
                                ->  XN Seq Scan on state_population sp  (cost=0.00..0.48 rows=48 width=37)
----- Tables missing statistics: state_population -----
----- Update statistics by running the ANALYZE command on these tables -----
----- Nested Loop Join in the query plan - review the join predicates to avoid Cartesian products -----

--6. Do everything above without dist key and do a explain  on the above query. (query 4). store in notepad for future ref.
EXPLAIN
WITH unique_dates AS (
    SELECT DISTINCT o_date
    FROM employee1
),
unique_employees AS (
    SELECT DISTINCT name
    FROM employee1
),
all_dates_employees AS (
    SELECT 
        d.o_date,
        e.name
    FROM 
        unique_dates d
    CROSS JOIN 
        unique_employees e
),
worked_dates AS (
    SELECT DISTINCT 
        name, 
        o_date 
    FROM 
        employee1
),
employees_did_not_work AS (
    SELECT 
        a.o_date,
        a.name
    FROM 
        all_dates_employees a
    LEFT JOIN 
        worked_dates w
    ON 
        a.o_date = w.o_date 
        AND a.name = w.name
    WHERE 
        w.name IS NULL
),
employees_did_not_work_count AS (
    SELECT 
        o_date,
        COUNT(*) AS employees_did_not_work
    FROM 
        employees_did_not_work
    GROUP BY 
        o_date
)
SELECT 
    e.o_date,
    e.employees_did_not_work,
    sp.state,
    sp.population
FROM 
    employees_did_not_work_count e
JOIN 
    employee emp ON e.o_date = emp.o_date
JOIN 
    state_population sp ON emp.state = sp.state
GROUP BY 
    e.o_date, sp.state, sp.population, e.employees_did_not_work
ORDER BY 
    e.o_date;




XN Merge  (cost=1000024798266.08..1000024798266.15 rows=26 width=49)
  Merge Key: e.o_date
  ->  XN Network  (cost=1000024798266.08..1000024798266.15 rows=26 width=49)
        Send to leader
        ->  XN Sort  (cost=1000024798266.08..1000024798266.15 rows=26 width=49)
              Sort Key: e.o_date
              ->  XN HashAggregate  (cost=24798265.47..24798265.47 rows=26 width=49)
                    ->  XN Hash Join DS_DIST_ALL_NONE  (cost=24018259.37..24798265.21 rows=26 width=49)
                          Hash Cond: (("outer".state)::text = ("inner".state)::text)
                          ->  XN Hash Join DS_DIST_INNER  (cost=24018258.77..24798264.03 rows=26 width=45)
                                Inner Dist Key: emp.o_date
                                Hash Cond: ("outer".o_date = "inner".o_date)
                                ->  XN Subquery Scan e  (cost=24018258.44..24018260.94 rows=200 width=12)
                                      ->  XN HashAggregate  (cost=24018258.44..24018258.94 rows=200 width=4)
                                            ->  XN Hash Left Join DS_DIST_ALL_NONE  (cost=9.24..24018058.44 rows=40000 width=4)
                                                  Hash Cond: ((("outer".name)::text = ("inner".name)::text) AND ("outer".o_date = "inner".o_date))
                                                  Filter: ("inner".name IS NULL)
                                                  ->  XN Nested Loop DS_BCAST_INNER  (cost=0.00..24001847.20 rows=40000 width=97)
                                                        ->  XN Subquery Scan e  (cost=0.00..7.20 rows=200 width=93)
                                                              ->  XN Unique  (cost=0.00..5.20 rows=200 width=93)
                                                                    ->  XN Seq Scan on employee1  (cost=0.00..4.16 rows=416 width=93)
                                                        ->  XN Subquery Scan d  (cost=0.00..7.20 rows=200 width=4)
                                                              ->  XN Unique  (cost=0.00..5.20 rows=200 width=4)
                                                                    ->  XN Seq Scan on employee1  (cost=0.00..4.16 rows=416 width=4)
                                                  ->  XN Hash  (cost=8.24..8.24 rows=200 width=97)
                                                        ->  XN Subquery Scan w  (cost=0.00..8.24 rows=200 width=97)
                                                              ->  XN Unique  (cost=0.00..6.24 rows=200 width=97)
                                                                    ->  XN Seq Scan on employee1  (cost=0.00..4.16 rows=416 width=97)
                                ->  XN Hash  (cost=0.26..0.26 rows=26 width=37)
                                      ->  XN Seq Scan on employee emp  (cost=0.00..0.26 rows=26 width=37)
                          ->  XN Hash  (cost=0.48..0.48 rows=48 width=37)
                                ->  XN Seq Scan on state_population sp  (cost=0.00..0.48 rows=48 width=37)
----- Tables missing statistics: state_population, employee1 -----
----- Update statistics by running the ANALYZE command on these tables -----
----- Nested Loop Join in the query plan - review the join predicates to avoid Cartesian products -----
--Task 2
--1. Use dist key and sory key  (dist is name and sort is odate)
CREATE TABLE employee2 (
    name VARCHAR(50) ENCODE zstd,
    address VARCHAR(100) ENCODE zstd,
    salary INT ENCODE az64,
    sex VARCHAR(10) ENCODE zstd,
    o_date DATE ENCODE az64,
    state VARCHAR(10) ENCODE zstd
)
DISTKEY(name)
SORTKEY(o_date);
CREATE TABLE state_population2 (
    state VARCHAR(10) ENCODE zstd,
    population INT ENCODE az64
)
DISTKEY(state)
SORTKEY(state);



XN Merge  (cost=1000005752545.28..1000005752545.29 rows=3 width=49)
  Merge Key: e.o_date
  ->  XN Network  (cost=1000005752545.28..1000005752545.29 rows=3 width=49)
        Send to leader
        ->  XN Sort  (cost=1000005752545.28..1000005752545.29 rows=3 width=49)
              Sort Key: e.o_date
              ->  XN HashAggregate  (cost=5752545.26..5752545.26 rows=3 width=49)
                    ->  XN Hash Join DS_DIST_INNER  (cost=5655044.55..5752545.23 rows=3 width=49)
                          Inner Dist Key: emp.o_date
                          Hash Cond: ("outer".o_date = "inner".o_date)
                          ->  XN Subquery Scan e  (cost=4875043.89..4875044.21 rows=26 width=12)
                                ->  XN HashAggregate  (cost=4875043.89..4875043.95 rows=26 width=4)
                                      ->  XN Hash Left Join DS_DIST_INNER  (cost=0.78..4875040.50 rows=676 width=4)
                                            Inner Dist Key: w.name
                                            Hash Cond: ((("outer".name)::text = ("inner".name)::text) AND ("outer".o_date = "inner".o_date))
                                            Filter: ("inner".name IS NULL)
                                            ->  XN Nested Loop DS_BCAST_INNER  (cost=0.00..3120022.56 rows=676 width=97)
                                                  ->  XN Subquery Scan e  (cost=0.00..0.58 rows=26 width=93)
                                                        ->  XN Unique  (cost=0.00..0.33 rows=26 width=93)
                                                              ->  XN Seq Scan on employee2  (cost=0.00..0.26 rows=26 width=93)
                                                  ->  XN Subquery Scan d  (cost=0.00..0.58 rows=26 width=4)
                                                        ->  XN Unique  (cost=0.00..0.33 rows=26 width=4)
                                                              ->  XN Seq Scan on employee2  (cost=0.00..0.26 rows=26 width=4)
                                            ->  XN Hash  (cost=0.65..0.65 rows=26 width=97)
                                                  ->  XN Subquery Scan w  (cost=0.00..0.65 rows=26 width=97)
                                                        ->  XN Unique  (cost=0.00..0.39 rows=26 width=97)
                                                              ->  XN Seq Scan on employee2  (cost=0.00..0.26 rows=26 width=97)
                          ->  XN Hash  (cost=780000.65..780000.65 rows=3 width=41)
                                ->  XN Hash Join DS_DIST_OUTER  (cost=0.04..780000.65 rows=3 width=41)
                                      Outer Dist Key: emp.state
                                      Hash Cond: (("outer".state)::text = ("inner".state)::text)
                                      ->  XN Seq Scan on employee2 emp  (cost=0.00..0.26 rows=26 width=37)
                                      ->  XN Hash  (cost=0.03..0.03 rows=3 width=37)
                                            ->  XN Seq Scan on state_population2 sp  (cost=0.00..0.03 rows=3 width=37)
----- Tables missing statistics: employee2, state_population2 -----
----- Update statistics by running the ANALYZE command on these tables -----
----- Nested Loop Join in the query plan - review the join predicates to avoid Cartesian products -----
EXPLAIN
WITH unique_dates AS (
    SELECT DISTINCT o_date
    FROM employee2
),
unique_employees AS (
    SELECT DISTINCT name
    FROM employee2
),
all_dates_employees AS (
    SELECT 
        d.o_date,
        e.name
    FROM 
        unique_dates d
    CROSS JOIN 
        unique_employees e
),
worked_dates AS (
    SELECT DISTINCT 
        name, 
        o_date 
    FROM 
        employee2
),
employees_did_not_work AS (
    SELECT 
        a.o_date,
        a.name
    FROM 
        all_dates_employees a
    LEFT JOIN 
        worked_dates w
    ON 
        a.o_date = w.o_date 
        AND a.name = w.name
    WHERE 
        w.name IS NULL
),
employees_did_not_work_count AS (
    SELECT 
        o_date,
        COUNT(*) AS employees_did_not_work
    FROM 
        employees_did_not_work
    GROUP BY 
        o_date
)
SELECT 
    e.o_date,
    e.employees_did_not_work,
    sp.state,
    sp.population
FROM 
    employees_did_not_work_count e
JOIN 
    employee2 emp ON e.o_date = emp.o_date
JOIN 
    state_population2 sp ON emp.state = sp.state
GROUP BY 
    e.o_date, sp.state, sp.population, e.employees_did_not_work
ORDER BY 
    e.o_date;

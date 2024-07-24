--1. How many employees are there by each stae and sex.
SELECT 
    e.state, 
    e.sex, 
    COUNT(*) AS employee_count,
    p.population
FROM 
    "07_24" e
JOIN 
    "state_population" p
ON 
    e.state = p.state
GROUP BY 
    e.state, e.sex, p.population;

--2. How many employeed worked only 2 days.
SELECT 
    name,
    COUNT(DISTINCT o_date) AS days_worked
FROM 
    "07_24"
GROUP BY 
    name
HAVING 
    COUNT(DISTINCT o_date) = 2;

--3. Show me the mployee who got max salary in each day.
WITH RankedSalaries AS (
    SELECT
        name,
        address,
        salary,
        sex,
        o_date,
        state,
        ROW_NUMBER() OVER (PARTITION BY o_date ORDER BY salary DESC) AS rank
    FROM
        "07_24"
)
SELECT
    name,
    address,
    salary,
    sex,
    o_date,
    state
FROM
    RankedSalaries
WHERE
    rank = 1;

--4. How  many employees in each day didnt get work ? (Here you have to join the tables)
WITH unique_dates AS (
    SELECT DISTINCT o_date
    FROM "07_24"
),
unique_employees AS (
    SELECT DISTINCT name
    FROM "07_24"
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
        "07_24"
)
SELECT 
    a.o_date,
    COUNT(*) AS employees_did_not_work
FROM 
    all_dates_employees a
LEFT JOIN 
    worked_dates w
ON 
    a.o_date = w.o_date 
    AND a.name = w.name
WHERE 
    w.name IS NULL
GROUP BY 
    a.o_date
ORDER BY 
    a.o_date;


--5. what the max salary difference in each day . (ex: day1:0, day2 : day2max-day1max)
WITH daily_max_salary AS (
    SELECT 
        o_date,
        MAX(salary) AS max_salary
    FROM 
        "07_24"
    GROUP BY 
        o_date
),
salary_differences AS (
    SELECT 
        o_date,
        max_salary,
        LAG(max_salary) OVER (ORDER BY o_date) AS previous_max_salary,
        COALESCE(max_salary - LAG(max_salary) OVER (ORDER BY o_date), 0) AS salary_difference
    FROM 
        daily_max_salary
)
SELECT 
    o_date,
    max_salary,
    previous_max_salary,
    salary_difference
FROM 
    salary_differences
ORDER BY 
    o_date;

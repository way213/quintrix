--1. How many female employees are there?
select COUNT(*) from "07_23"
where sex = 'female';
--2. What is the max salary of female employee?
select MAX(salary) from "07_23"
where sex = 'female';
--3. What is the max salary of male employee?
select MAX(salary) from "07_23"
where sex = 'male';
--4. Give me 5 rows from the table sorting by salary (high to low)
select * from "07_23"
order by salary DESC
limit 5;
--5. How many employees are having same salary?
SELECT salary, COUNT(*) as employee_count
FROM "07_23"
GROUP BY salary
HAVING COUNT(*) > 1;

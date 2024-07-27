from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearning") \
      .getOrCreate()

# Read the CSV files
df_users = spark.read.csv("/Users/williamyang/Documents/GitHub/quintrix/Session5/user_data.csv", header=True, inferSchema=True)
df_country = spark.read.csv("/Users/williamyang/Documents/GitHub/quintrix/Session5/states_population.csv", header=True, inferSchema=True)

# Write the DataFrames to Parquet files
user_parquet_path = "/Users/williamyang/Documents/GitHub/quintrix/Session5/user_data.parquet"
country_parquet_path = "/Users/williamyang/Documents/GitHub/quintrix/Session5/states_population.parquet"

df_users.write.parquet(user_parquet_path, mode='overwrite')
df_country.write.parquet(country_parquet_path, mode='overwrite')

# Read the Parquet files
df_users_parquet = spark.read.parquet(user_parquet_path)
df_country_parquet = spark.read.parquet(country_parquet_path)

# Show the DataFrames read from Parquet
# df_users_parquet.show(10, False)
# df_country_parquet.show(10, False)

# Create temporary views
df_users_parquet.createOrReplaceTempView('user_data_view')
df_country_parquet.createOrReplaceTempView('country_data_view')

# query 1
spark.sql("""
    SELECT 
        e.state, 
        e.sex, 
        COUNT(*) AS employee_count,
        p.population
    FROM 
        user_data_view e
    JOIN 
        country_data_view p
    ON 
        e.state = p.state
    GROUP BY 
        e.state, e.sex, p.population
""").show(10, False)
# query 2
spark.sql("""
SELECT 
    name,
    COUNT(DISTINCT o_date) AS days_worked
FROM 
    user_data_view
GROUP BY 
    name
HAVING 
    COUNT(DISTINCT o_date) = 2;""").show()


# query 3
spark.sql("""
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
        user_data_view
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
    rank = 1;""").show()
# query 4
spark.sql("""
WITH unique_dates AS (
    SELECT DISTINCT o_date
    FROM user_data_view
),
unique_employees AS (
    SELECT DISTINCT name
    FROM user_data_view
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
        user_data_view
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
    a.o_date;""").show()

# query 5
spark.sql("""
WITH daily_max_salary AS (
    SELECT 
        o_date,
        MAX(salary) AS max_salary
    FROM 
        user_data_view
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
""").show()
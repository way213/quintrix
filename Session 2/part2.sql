--1.Join tran_fact and cust_dim_details on cust_id and tran_dt between start_date and end_date
SELECT 
    *
FROM 
    cards_ingest.tran_fact t
JOIN 
    cards_ingest.cust_dim_details c
ON 
    t.cust_id = c.cust_id
AND 
    t.tran_date BETWEEN c.start_date AND c.end_date;
--2.show me all the fields and total tansaction ammount per tran_date and only 2nd rank of the transaction
WITH RankedTransactions AS (
    SELECT 
        t.tran_id, 
        t.cust_id, 
        t.stat_cd, 
        t.tran_ammt, 
        t.tran_date,
        SUM(t.tran_ammt) OVER (PARTITION BY t.tran_date) AS total_tran_ammt_per_date,
        RANK() OVER (PARTITION BY t.tran_date ORDER BY t.tran_ammt DESC) AS tran_ammt_rank
    FROM 
        cards_ingest.tran_fact t
)
SELECT 
    rt.tran_id, 
    rt.cust_id, 
    rt.stat_cd, 
    rt.tran_ammt, 
    rt.tran_date,
    rt.total_tran_ammt_per_date,
    rt.tran_ammt_rank,
    c.start_date,
    c.end_date
FROM 
    RankedTransactions rt
JOIN 
    cards_ingest.cust_dim_details c
ON 
    rt.cust_id = c.cust_id
AND 
    rt.tran_date BETWEEN c.start_date AND c.end_date
WHERE 
    rt.tran_ammt_rank = 2;
--3.when stat_cd is not euqal to state_cd then data issues else good data as stae_cd_status
WITH RankedTransactions AS (
    SELECT 
        t.tran_id, 
        t.cust_id, 
        t.stat_cd, 
        t.tran_ammt, 
        t.tran_date,
        SUM(t.tran_ammt) OVER (PARTITION BY t.tran_date) AS total_tran_ammt_per_date,
        RANK() OVER (PARTITION BY t.tran_date ORDER BY t.tran_ammt DESC) AS tran_ammt_rank
    FROM 
        cards_ingest.tran_fact t
)
SELECT 
    rt.tran_id, 
    rt.cust_id, 
    rt.stat_cd, 
    rt.tran_ammt, 
    rt.tran_date,
    rt.total_tran_ammt_per_date,
    rt.tran_ammt_rank,
    c.start_date,
    c.end_date,
    CASE 
        WHEN rt.stat_cd = c.state_cd THEN 'Good Data'
        ELSE 'Data Issues'
    END AS state_cd_status
FROM 
    RankedTransactions rt
JOIN 
    cards_ingest.cust_dim_details c
ON 
    rt.cust_id = c.cust_id
AND 
    rt.tran_date BETWEEN c.start_date AND c.end_date
WHERE 
    rt.tran_ammt_rank = 2;

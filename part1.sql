-- 1.show me all the tran_date,tran_ammt and total tansaction ammount per tran_date
 SELECT 
    tran_date, 
    tran_ammt, 
    SUM(tran_ammt) OVER (PARTITION BY tran_date) AS total_tran_ammt_per_date
FROM 
    cards_ingest.tran_fact;

--2.show me all the tran_date,tran_ammt and total tansaction ammount per tran_date and rank of the transaction ammount desc within per t
SELECT 
    tran_date, 
    tran_ammt, 
    SUM(tran_ammt) OVER (PARTITION BY tran_date) AS total_tran_ammt_per_date,
    RANK() OVER (PARTITION BY tran_date ORDER BY tran_ammt DESC) AS tran_ammt_rank
FROM 
    cards_ingest.tran_fact;
--3.show me all the fields and total tansaction ammount per tran_date and only 2nd rank of the transaction ammount desc within per tran_date
WITH RankedTransactions AS (
    SELECT 
        tran_id, 
        cust_id, 
        stat_cd, 
        tran_ammt, 
        tran_date,
        SUM(tran_ammt) OVER (PARTITION BY tran_date) AS total_tran_ammt_per_date,
        RANK() OVER (PARTITION BY tran_date ORDER BY tran_ammt DESC) AS tran_ammt_rank
    FROM 
        cards_ingest.tran_fact
)
SELECT 
    tran_id, 
    cust_id, 
    stat_cd, 
    tran_ammt, 
    tran_date,
    total_tran_ammt_per_date,
    tran_ammt_rank
FROM 
    RankedTransactions
WHERE 
    tran_ammt_rank = 2;

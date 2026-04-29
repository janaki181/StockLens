
select * from price_history;

select * from raw_stock_data;

select * from stock_data;

ALTER TABLE price_history ADD COLUMN vs_nifty_cumulative FLOAT;

select ticker, count(date)
from price_history
group by ticker;

drop database nifty_competitor_analysis;

SELECT datname FROM pg_database;

SELECT current_database();
SELECT DISTINCT date
FROM price_history
ORDER BY date DESC;

SELECT DISTINCT date
FROM stock_data
ORDER BY date DESC;

SELECT * FROM price_history 
WHERE DATE(date) = CURRENT_DATE;

SELECT * FROM stock_data 
WHERE DATE(date) = CURRENT_DATE-2;

select * from price_history
where ticker = 'AXISBANK';

select * from stock_data
where volume_ratio > 2;
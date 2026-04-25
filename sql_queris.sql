
select * from price_history;

select * from raw_stock_data;

select * from stock_data;

ALTER TABLE price_history ADD COLUMN vs_nifty_cumulative FLOAT;

select ticker, count(date)
from price_history
group by ticker;

drop database nifty_competitor_analysis;

SELECT datname FROM ;

SHOW DATABASES;
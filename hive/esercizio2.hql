DROP TABLE receipts;
DROP TABLE prices;
DROP TABLE product_monthly_total;

CREATE TABLE receipts (data STRING, products Array<STRING>) row format delimited
fields terminated by ':'
collection items terminated by ',';

-- LOAD DATA local INPATH '/home/luca/Desktop/hive/spesa.txt'
-- LOAD DATA INPATH 'input/hive/spesa.txt'
-- LOAD DATA local INPATH '/pico/home/usertrain/a08trb02/input/hive/spesa.txt'

LOAD DATA local INPATH '/home/andybergon/input/hive/spesa.txt'
OVERWRITE INTO TABLE receipts;

CREATE TABLE prices (product STRING, price STRING) row format delimited
fields terminated by ',';

-- LOAD DATA local INPATH '/home/luca/Desktop/hive/prices.txt'
-- LOAD DATA INPATH 'input/hive/prices.txt'
-- LOAD DATA local INPATH '/pico/home/usertrain/a08trb02/input/hive/prices.txt'

LOAD DATA local INPATH '/home/andybergon/input/hive/prices.txt'
OVERWRITE INTO TABLE prices;


CREATE TABLE product_monthly_total AS
SELECT z.prod AS prod, collect_set(CONCAT(z.data,":",z.total_price)) AS month_total
FROM
  (SELECT y.data AS data, y.prod AS prod, y.tot AS tot, price AS unit_price, y.tot*price AS total_price
  FROM
    (SELECT x.data AS data, x.prod AS prod, count(1) AS tot
    FROM
      (SELECT date_format(data, "MM/yyyy") AS data, prod
      FROM receipts LATERAL VIEW explode(products) subA AS prod) x
    GROUP BY data, prod
    ORDER BY data ASC, prod ASC) y, prices
  WHERE prod = product) z
GROUP BY prod;


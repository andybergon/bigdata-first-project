DROP TABLE products;
DROP TABLE dateproduct;

CREATE TABLE products (data STRING, list Array<STRING>) row format delimited
fields terminated by ':'
collection items terminated by ',';


-- LOAD DATA local INPATH '/home/luca/Desktop/hive/spesa.txt'
-- LOAD DATA local INPATH '/home/andybergon/input/hive/spesa.txt'
-- LOAD DATA local INPATH '/pico/home/usertrain/a08trb02/input/hive/spesa.txt'

LOAD DATA INPATH '/user/hive/input/spesa.txt'
OVERWRITE INTO TABLE products;

CREATE TABLE dateproduct AS
SELECT x.data, x.coll[0], x.coll[1], x.coll[2], x.coll[3], x.coll[4]
FROM (SELECT s.data AS data, collect_set(CONCAT(s.prod," ",s.count,", ")) AS coll
    FROM (SELECT w.data, w.prod, count(1) AS count
        FROM (SELECT date_format(data, "yyyy-MM") as data, prod
            FROM products LATERAL VIEW explode(list) subA AS prod) w
        GROUP BY w.data, w.prod
        ORDER BY count DESC) s
    GROUP BY s.data) x;

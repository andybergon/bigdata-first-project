DROP TABLE receipts;
DROP TABLE receipts1;
DROP TABLE pairs_in_receipt;

CREATE TABLE receipts1 (data STRING, list Array<STRING>) row format delimited
fields terminated by ':'
collection items terminated by ',';

-- LOAD DATA local INPATH '/home/luca/Desktop/hive/spesa.txt'
LOAD DATA INPATH '/input/hive/spesa.txt'
OVERWRITE INTO TABLE receipts1;

CREATE TABLE receipts
AS
SELECT 
  reflect('java.util.UUID','randomUUID') as id,
  *
FROM
  receipts1;

CREATE TABLE pairs_in_receipt AS
SELECT f.prod1, f.prod2, f.perc1, concat(f.count/v.occurency*100,'%')
FROM
  (SELECT z.prod1 AS prod1, z.prod2 AS prod2, count, concat(count/x.countot*100,'%') AS perc1
  FROM (SELECT w.prod AS prod1, y.prod AS prod2, count(1) AS count
        FROM (SELECT id, prod
              FROM receipts LATERAL VIEW explode(list) subA AS prod) w
        JOIN (SELECT id, prod
              FROM receipts LATERAL VIEW explode(list) subA AS prod) y
        ON w.id=y.id
        WHERE w.prod!=y.prod
        GROUP BY w.prod, y.prod) z
  JOIN (SELECT count(1) AS countot
        FROM receipts) x) f
JOIN (SELECT prod, count(1) AS occurency
      FROM receipts LATERAL VIEW explode(list) subA AS prod
      GROUP BY prod) v
ON f.prod1=v.prod;


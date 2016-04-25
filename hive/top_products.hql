# date - item
# 2015-02-11  pane
# 2015-02-11  formaggio
# 2015-09-28  pesce

# come popolo la tabella? serve classe java?
CREATE TABLE IF NOT EXISTS receipt (
  date DATE,
  item STRING
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/input/spesa.txt' INTO TABLE receipt;
-- LOAD DATA LOCAL INPATH '/home/andybergon/Desktop/hive-top_products/spesa.txt' INTO TABLE receipt;


-- add jar PrimoProgetto-0.0.1-SNAPSHOT.jar;
-- CREATE TEMPORARY FUNCTION <a> AS 'hive.topproducts.TopProducts';

SELECT date, product, COUNT(product) AS quantity
FROM receipt
GROUP BY date
SORT BY quantity DESC
LIMIT 5;

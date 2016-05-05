DROP TABLE receipts;
DROP TABLE id_product_list;
DROP TABLE id_product;
DROP TABLE product_occurrences;
DROP TABLE product_couples;
DROP TABLE product_pairs_occurences;
DROP TABLE receipt_number;
DROP TABLE pairs_in_receipt;

CREATE TABLE receipts (data Date, products STRING)
row format delimited
fields terminated by ':';

-- LOAD DATA local INPATH '/home/luca/Desktop/hive/spesa.txt'
-- LOAD DATA INPATH '/input/hive/spesa.txt'
-- LOAD DATA local INPATH '/pico/home/usertrain/a08trb02/input/hive/spesa.txt'

LOAD DATA local INPATH '/home/andybergon/input/hive/spesa.txt'
OVERWRITE INTO TABLE receipts;

CREATE TABLE id_product_list AS
SELECT ROW_NUMBER() OVER() AS id, products
FROM receipts;

-- SELECT *
-- FROM id_product_list
-- WHERE id < 100;

CREATE TABLE id_product AS
SELECT id, product
FROM id_product_list LATERAL VIEW explode(split(products,',')) partial AS product;

-- SELECT *
-- FROM id_product
-- WHERE id < 100;

CREATE TABLE product_occurrences AS
SELECT product, count(1) AS occurrences_single
FROM id_product
GROUP BY product;

-- SELECT *
-- FROM product_occurrences;

CREATE TABLE product_couples AS
SELECT x.id AS id, x.product AS product1, y.product AS product2
FROM id_product x, id_product y
WHERE x.id=y.id
AND x.product!=y.product;

-- SELECT *
-- FROM product_couples
-- WHERE id < 100;

CREATE TABLE product_pairs_occurences AS
SELECT product1, product2, COUNT(1) AS occurrences_pair
FROM product_couples
GROUP BY product1, product2;

-- SELECT *
-- FROM product_pairs_occurences;

CREATE TABLE receipt_number AS
SELECT MAX(id) AS number_of_receipts
FROM id_product;

-- SELECT *
-- FROM receipt_number;

CREATE TABLE pairs_in_receipt AS
SELECT product1, product2, (occurrences_pair/number_of_receipts) AS support, (occurrences_pair/occurrences_single) AS confidence
FROM product_pairs_occurences, product_occurrences, receipt_number
WHERE product_occurrences.product=product1;

-- SELECT * FROM pairs_in_receipt;

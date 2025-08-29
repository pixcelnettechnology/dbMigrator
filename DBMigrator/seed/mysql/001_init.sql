CREATE TABLE IF NOT EXISTS types_demo (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  amount DECIMAL(38,9),
  payload LONGBLOB,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  notes LONGTEXT,
  meta JSON
);
INSERT INTO types_demo (name, active, amount, payload, notes, meta)
SELECT CONCAT('row_', g), IF(g % 2 = 0, 1, 0),
       g * 1.23456789, REPEAT('A', 16),
       IF(g % 5 = 0, REPEAT('x', 1000), NULL),
       JSON_OBJECT('i', g, 's', CONCAT('v', g))
FROM (SELECT 1 g UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) AS t;
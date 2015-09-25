-- ===================================================================
-- create FDW objects
-- ===================================================================

-- Error, extension isn't installed yet
ALTER SERVER loopback OPTIONS (ADD extensions 'seg');

-- Try again
CREATE EXTENSION seg;
ALTER SERVER loopback OPTIONS (ADD extensions 'seg');
ALTER SERVER loopback OPTIONS (DROP extensions);


-- ===================================================================
-- create objects used through FDW loopback server
-- ===================================================================

CREATE SCHEMA "SH 1";
CREATE TABLE "SH 1"."TBL 1" (
	"C 1" int NOT NULL,
	c2 int NOT NULL,
	c3 seg,
	c4 timestamptz
);

INSERT INTO "SH 1"."TBL 1"
	SELECT id,
	       2 * id,
	       (id || ' .. ' || 2*id)::seg,
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval
	FROM generate_series(1, 1000) id;

ANALYZE "SH 1"."TBL 1";

-- ===================================================================
-- create foreign table
-- ===================================================================

CREATE FOREIGN TABLE shft1 (
	"C 1" int NOT NULL,
	c2 int NOT NULL,
	c3 seg,
	c4 timestamptz
) SERVER loopback
OPTIONS (schema_name 'SH 1', table_name 'TBL 1');

-- ===================================================================
-- simple queries
-- ===================================================================

-- without operator shipping
EXPLAIN (COSTS false) SELECT * FROM shft1 LIMIT 1;
EXPLAIN VERBOSE SELECT c2 FROM shft1 WHERE c3 && '1.5 .. 2.5'::seg;
SELECT c2 FROM shft1 WHERE c3 && '1.5 .. 2.5'::seg;

-- with operator shipping
ALTER SERVER loopback OPTIONS (ADD extensions 'seg');
EXPLAIN VERBOSE SELECT c2 FROM shft1 WHERE c3 && '1.5 .. 2.5'::seg;
SELECT c2 FROM shft1 WHERE c3 && '1.5 .. 2.5'::seg;


-- ===================================================================
-- clean up
-- ===================================================================




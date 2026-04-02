--
-- CedarDB dialect template for TPC-DS dsqgen.
-- CedarDB is PostgreSQL-compatible; LIMIT clause goes at the end of the query.
--
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = " limit %d";

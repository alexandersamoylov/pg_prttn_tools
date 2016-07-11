EXTENSION = pg_prttn_tools
DATA = sql/pg_prttn_tools--*.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

PG_CONFIG = pg_config
CXXFLAGS = -std=c++11 -I $(shell $(PG_CONFIG) --includedir) -g
LDFLAGS = -L $(shell $(PG_CONFIG) --libdir) -lpq

pg_queue_bench: pg_queue_bench.cpp
	$(CXX) -Wall -Werror $(CXXFLAGS) pg_queue_bench.cpp -o pg_queue_bench $(LDFLAGS)

clean:
	rm -fr pg_queue_bench.o pg_queue_bench

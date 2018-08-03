/*
 * Some simple benchmarks of queue-like workloads in PostgreSQL.
 */

#include "libpq-fe.h"

#include <poll.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int usage(const std::string& name)
{
  std::cerr << "Usage: " << name << '\n'
			<< " --producers N    number of producer threads\n"
			<< " --consumers N    number of consumer threads\n"
			<< " --messages N     number of messages per producer\n"
			<< " --no-pk          no primary key\n"
			<< " --wait           wait for producers before consuming\n"
			<< " --fifo           ordered by time (extra index)\n"
			<< " --zheap          use zheap table\n"
			<< " --connstr X      connection string\n";
  return EXIT_FAILURE;
}

enum mode_type {
  MODE_SIMPLE,
  MODE_FOR_UPDATE,
  MODE_FOR_UPDATE_SKIP_LOCKED
};

void do_consumer(const std::string& connstr, bool fifo, mode_type mode)
{
  char query[1024];

  bool maybe_ready = false;
  bool shutdown_received = false;
  auto conn = PQconnectdb(connstr.c_str());
  assert(PQstatus(conn) == CONNECTION_OK);
  auto result = PQexec(conn, "LISTEN sms_queue_broadcast");
  assert(PQresultStatus(result) == PGRES_COMMAND_OK);
  
  snprintf(query, sizeof(query),
		   "SELECT ctid, destination, message "
		   "FROM sms_queue "
		   "%s "
		   "%s "
		   "LIMIT 1",
		   fifo ? "ORDER BY time" : "",
		   mode == MODE_FOR_UPDATE ? "FOR UPDATE" :
		   mode == MODE_FOR_UPDATE_SKIP_LOCKED ? "FOR UPDATE SKIP LOCKED" :
		   "");
  result = PQprepare(conn, "my_select", query, 0, NULL);
  assert(PQresultStatus(result) == PGRES_COMMAND_OK);

  Oid zero = 0;
  result = PQprepare(conn, "my_delete",
					 "DELETE FROM sms_queue WHERE ctid = $1",
					 1, &zero);
  assert(PQresultStatus(result) == PGRES_COMMAND_OK);
  
  while (maybe_ready || !shutdown_received) {
	result = PQexec(conn, "BEGIN");	
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);
	result = PQexecPrepared(conn, "my_select",
							0, NULL, NULL, NULL, 0);
	assert(PQresultStatus(result) == PGRES_TUPLES_OK);
	if (PQntuples(result) == 1) {
	  // delete the message
	  char ctid[80];
	  strncpy(ctid, PQgetvalue(result, 0, 0), sizeof(ctid));
	  PQclear(result);
	  const char *ctidp[] = { ctid };
	  result = PQexecPrepared(conn,
							  "my_delete",
							  1, ctidp, NULL, NULL, 0);
	  assert(PQresultStatus(result) == PGRES_COMMAND_OK);
	  maybe_ready = true; // always poll again immediately when successful
	} else {
	  // no message in queue -- we need to wait for notifications
	  maybe_ready = false;
	}
	result = PQexec(conn, "COMMIT");
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);

	// consume any NOTIFY messages
	PQconsumeInput(conn);
	while (PGnotify *notify = PQnotifies(conn)) {
	  if (notify->extra && strcmp(notify->extra, "shutdown") == 0) {
		shutdown_received = true;
	  }
	  maybe_ready = true;
	  PQfreemem(notify);
	}
	if (!maybe_ready && !shutdown_received) {
	  struct pollfd s = { PQsocket(conn), POLLIN, 0 };
	  poll(&s, 1, -1);
	}
  }
}

void do_producer(const std::string& connstr, int messages)
{
  auto conn = PQconnectdb(connstr.c_str());
  assert(PQstatus(conn) == CONNECTION_OK);
  for (int i = 0; i < messages; ++i) {
	auto result = PQexec(conn, "BEGIN");
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);
	result = PQexec(conn,
					"INSERT INTO sms_queue (destination, message, time) "
					"VALUES ('+1 555 123-4567', 'hello world', now())");
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);
	result = PQexec(conn, "NOTIFY sms_queue_broadcast");
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);
	result = PQexec(conn, "COMMIT");
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);
  }
}

int main(int argc, char *argv[])
{
  // how to connect to the database
  std::string connstr = "dbname=postgres";
  // which type of test to run
  mode_type mode = MODE_SIMPLE;
  // how many consumer threads
  int nconsumers = 1;
  // how many producer threads
  int nproducers = 1;
  // how many messages each producer should send
  int nmessages = 1000;
  // whether to use (approx) fifo ordering (an index) or unordered
  bool fifo = false;
  // whether to produce-wait-consume, or produce/consume concurrently
  bool wait = false;
  // whether to have a primary key
  bool primary_key = true;
  // whether to use zheap
  bool zheap = false;

  // Parse command line options.
  for (int i = 1; i < argc; ++i) {
	bool more = i < argc + 1;
	std::string arg = argv[i];
	if (arg == "--fifo") {
	  fifo = true;
	} else if (arg == "--for-update") {
	  mode = MODE_FOR_UPDATE;
	} else if (arg == "--for-update-skip-locked") {
	  mode = MODE_FOR_UPDATE_SKIP_LOCKED;
	} else if (arg == "--consumers" && more) {
	  nconsumers = std::atoi(argv[++i]);
	} else if (arg == "--producers" && more) {
	  nproducers = std::atoi(argv[++i]);
	} else if (arg == "--messages" && more) {
	  nmessages = std::atoi(argv[++i]);
	} else if (arg == "--connstr" && more) {
	  connstr = argv[++i];
	} else if (arg == "--wait") {
	  wait = true;
	} else if (arg == "--no-pk") {
	  primary_key = false;
	} else if (arg == "--zheap") {
	  zheap = true;
	} else {
	  return usage(argv[0]);
	}
  }

  // prepare the database
  auto conn = PQconnectdb(connstr.c_str());
  assert(PQstatus(conn) == CONNECTION_OK);
  auto result = PQexec(conn, "DROP TABLE IF EXISTS sms_queue");
  assert(PQresultStatus(result) == PGRES_COMMAND_OK);
  std::string create;
  if (primary_key)
    create = "CREATE TABLE sms_queue(id SERIAL PRIMARY KEY, destination TEXT NOT NULL, message TEXT NOT NULL, time TIMESTAMP WITH TIME ZONE NOT NULL)";
  else
    create = "CREATE TABLE sms_queue(id SERIAL, destination TEXT NOT NULL, message TEXT NOT NULL, time TIMESTAMP WITH TIME ZONE NOT NULL)";
  if (zheap)
    create += " WITH (storage_engine = zheap)";
  result = PQexec(conn, create.c_str());
  assert(PQresultStatus(result) == PGRES_COMMAND_OK);
  if (fifo) {
	result = PQexec(conn, "CREATE INDEX ON sms_queue(time)");
	assert(PQresultStatus(result) == PGRES_COMMAND_OK);
  }

  // start all the producer threads
  std::vector<std::thread> producers;
  for (auto i = 0; i < nproducers; ++i)
	producers.push_back(std::thread([&](){do_producer(connstr, nmessages / nproducers);}));

  // if asked to wait, wait for them to finish
  if (wait) {
	for(auto& thread: producers)
	  thread.join();
	producers.clear();
  }
  
  auto consumer_start = std::chrono::steady_clock::now();
  // start all the consumer threads
  std::vector<std::thread> consumers;
  for (auto i = 0; i < nconsumers; ++i)
	consumers.push_back(std::thread([&](){ do_consumer(connstr, fifo, mode); }));

  // wait for all the producers to finish (if we didn't already)
  for(auto& thread: producers)
	thread.join();

  // tell the consumers to shut down
  // (after waiting for a moment to make sure they have had time to start listening,
  // which is a terrible hack, fixable with some interlocking)
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  result = PQexec(conn, "NOTIFY sms_queue_broadcast, 'shutdown'");
  assert(PQresultStatus(result) == PGRES_COMMAND_OK);

  // wait for the consumers to finish
  for(auto& thread: consumers)
	thread.join();

  auto consumer_finish = std::chrono::steady_clock::now();
  auto consumer_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(consumer_finish - consumer_start).count();
  std::cout << nmessages << " messages consumed in " << consumer_elapsed << "us (" << (consumer_elapsed / nmessages) << "us each, " << (nmessages / (consumer_elapsed / 1000000.0)) << " messages/sec)\n";

  PQfinish(conn);
}

#!/bin/bash

#options="--no-pk --tid"
options=""
messages=10000
#for lock_mode in "" --for-update --for-update-skip-locked ; do
for lock_mode in "" ; do
#for lock_mode in "" ; do
  echo "===== LOCK MODE: $lock_mode ====="
  echo "Threads: heap TPS -> zheap TPS"
  for consumers in 1 2 3 4 5 6 7 8 ; do
    echo -n "$consumers: "
    echo -n "` ./pg_queue_bench --messages $messages --wait --consumers $consumers $options $lock_mode  | cut -d' ' -f8 ` -> "
    echo    "` ./pg_queue_bench --messages $messages --wait --consumers $consumers $options $lock_mode  --zheap | cut -d' ' -f8 `"
  done
  echo
done

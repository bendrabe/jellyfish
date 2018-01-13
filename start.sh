#!/bin/bash
./a.out create
for i in {0..4}; do ./a.out run ${i} $((RANDOM % 100)) 30 neighbors.txt & done
wait
./a.out destroy

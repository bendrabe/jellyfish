#!/bin/bash
for i in {0..4}; do ./a.out ${i} $((RANDOM % 100)) 10 neighbors.txt & done

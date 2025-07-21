#!/bin/bash

for i in {1..100}; do
    echo "test loop ($i)"
    go test -v
done

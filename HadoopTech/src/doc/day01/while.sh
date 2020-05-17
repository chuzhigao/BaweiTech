#!/bin/bash
i=1
while [[ $i -lt 5 ]]; do

    if [[ $i -eq 4 ]]; then
        echo "zhigao"
    else
        echo $i
    fi
    i=$[ i + 1 ]
done

#!/bin/bash
name="zhigao"
i=1
while [[ $i <=5 ]]; do
	if [[ $i -ne 4 ]]; then
		echo $i
	elif [[ $i -eq 4 ]]; then
	    echo $name
	fi
	i=$[ i + 1 ]
done
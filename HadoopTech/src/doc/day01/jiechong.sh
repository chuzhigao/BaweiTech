#!/bin/bash
bl=$1
cj=1

for (( i = 1; i <= $bl; i++ )); do

	if [[ $bl -lt 1 ]]; then
		echo $cj
	elif [[ $bl -gt 1 ]]; then
		cj=$[ cj * $i ]
	fi
done
echo $cj
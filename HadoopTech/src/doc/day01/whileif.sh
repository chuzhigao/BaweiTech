#!/bin/bash
while [[ true ]]; do

	read -p "请输入1 2 3 选择" choose
	if [[ $choose -eq 1 ]]; then
		echo "你是好人"
	elif [[ $choose -eq 2 ]]; then
	    echo "你是坏人"
	elif [[ $choose -eq 3 ]]; then
		echo "你是人吗？"
	fi

done
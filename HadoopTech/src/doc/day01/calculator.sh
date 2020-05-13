#!/bin/bash
read -p "请输入第一个参数a " a
read -p "请输入第二个参数b"  b
read -p "请输入第单个加减乘出 + - * /" fh

if [[ "$fh" == "+" ]]; then
	echo $[ a + b ]
elif [[ "$fh" == "-" ]]; then
    echo $[ a - b ]
elif [[ "$fh" == "*" ]]; then
	echo $[ a * b ]
elif [[ "$fh" == "/" ]]; then
	if [[ "$b" -eq 0 ]]; then
       echo "除法分母不能为0"
	elif [[ condition ]]; then
	   echo $[ a / b ]
	fi
				#statements
fi

#!/bin/bash
while [[ true ]]; do
	read -p "请输入1 2 3 三个数字选项" a
	if [[ $a -eq 1 ]]; then
		echo "你赢了"

	elif [[ $a -eq 2 ]]; then
		echo "你输了"
	elif [[ $a -eq 3 ]]; then
		echo "平局"
	else
		echo "非法输入退出"
		break

	fi


done

删除用户


#!/bin/bash
userdel chuzhigao

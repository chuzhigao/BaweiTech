#!/bin/bash
a=10
b=12

if [[ $a > $b ]]; then

    echo "a> b"
elif [[ $a == $b ]]; then
    echo "a==b"
elif [[ $a < $b ]]; then
    echo "a<b"
fi


#!/bin/bash
a=10
b=100
if [[ $a -gt $b ]]; then
        echo "$a>$b"
elif [[ $a  -eq $b ]]; then
        echo "a==b"
elif [[ $a -lt $b ]]; then
        echo "$a<$b"
fi

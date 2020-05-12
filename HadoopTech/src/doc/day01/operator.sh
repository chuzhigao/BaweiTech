#!/bin/bash
a=20
b=2
val=`expr $a + $b`
echo $val

d=`expr $a / $b`

echo $d
cf=`expr $a \* $b`
echo $cf

jf=`expr $a - $b`
echo $jf
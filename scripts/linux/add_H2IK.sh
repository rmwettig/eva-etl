#!/bin/bash 

for i in $(ls "$1");
do
  awk '{print '999999999', $0}'  FS=';' OFS=';' "$1/$i" > "$1/tmp";
  mv "$1/tmp" "$1/$i";
  sed -i '1s/999999999/H2IK/' "$1/$i"
done
exit 1


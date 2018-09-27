#!/bin/bash 

for i in $(ls "$1");
do
	tmpFile="$1/$i.tmp";
	awk '{print '999999999', $0}'  FS=';' OFS=';' "$1/$i" > "$tmpFile";
	mv "$tmpFile" "$1/$i";
	sed -i '1s/999999999/H2IK/' "$1/$i"
	echo "$i fixed."
done


#!/bin/bash

# Remove duplicated KV column

awk '{
  for(i=1;i<=NF;i++)
    if(i!=(NF-1)){
      if(i==NF){
        printf $i"\n"
      }
      if(i!=NF){
        printf $i";"
      }
    }
}' FS=";" "$1" > "$1.fixed"

mv "$1" "$1.bak"
mv "$1.fixed" "$1"


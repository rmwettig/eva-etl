#!/bin/bash

awk '{
  for(i=1; i<=NF; i++) {
    if(i==7) {
      if($i == "(")
        printf ""
      else
        printf $i
    }
    else {
      printf $i
    }
    if(i != NF) {
      printf ";"
    }
    else {
      printf "\n"
    }
  }
}' FS=";" "$1" > "$1.fixed"

mv "$1" "$1.bak"
mv "$1.fixed" "$1"
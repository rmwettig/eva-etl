#!/bin/bash

# This script extracts rows for the given health insurances.
# For each health insurance a separate folder is created.
# This folder contains the corresponding subsets for each ADB table.

# define hi as a map
typeset -A hi
hi=(["Bosch"]="108036123" ["Salzgitter"]="101922757")

# read user arguments
inDirectory="$1"
outDirectory="$2"
archiveDirectory=$inDirectory/archive

# check paramters
if [ $# -ne 2 ] || [ -z $inDirectory ] || [ -z $outDirectory ]; then
  echo "Invalid arguments. Call $0 <inDirectory> <outDirectory>."
  exit
fi

# create directories if necessary
if [ ! -d $outDirectory ]; then
  mkdir $outDirectory
fi

if [ ! -d $archiveDirectory ]; then
  mkdir $archiveDirectory
fi

# for each file in the input directory
for t in $(ls $inDirectory/*.csv)
do
    ifile=$inDirectory/$t
    # skip directories
    if [ -d $ifile ]; then
      continue
    fi
    echo "Processing $ifile..."
    
    # extract the header of the table
    header=`head -n 1 $inDirectory/$t`
    
    # for each health insurance in the map
    for k in "${!hi[@]}"
    do
      subOutDirectory=$outDirectory/$k
      # create insurance folder if needed
      if [ ! -d $subOutDirectory ]; then
          mkdir $subOutDirectory
      fi
      ofile=$subOutDirectory/$t
      
    # convert encoding to utf-8
    # get the row for the health incurance's h2ik
    iconv -f MS-ANSI -t UTF-8 $inDirectory/$t | grep "${hi[$k]}" > $ofile
      
    # add header
    sed -i 1i$header $ofile
    done
    
    # compress table file
    # and move to archive folder
    gzip $ifile
    mv $ifile.gz $archiveDirectory/$t.gz
done


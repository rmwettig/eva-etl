#!/bin/bash

# Call this from the dump root folder

java -jar eva-data.jar -makejob P:\eva-dump-configs\production\dump-adb.json
fexp -e logs/dump-fdb-destatis.err < P:\eva-dump-configs\production\fexp-jobs\eva-dump-adb.fx
java -jar eva-data.jar -clean P:\eva-dump-configs\production\dump-adb.json
. /p/workspace/projects/eva-data/scripts/remove_duplicated_kv.sh out/ACC_ADB/ACC_ADB_AVK_ADB_T_Arzt_GOP.csv
. /p/workspace/projects/eva-data/scripts/separate_HI.sh out/ACC_ADB out/ACC_ADB
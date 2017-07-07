#!/bin/bash

# Call this from the dump root folder

java -jar eva-data.jar -makejob P:\eva-dump-configs\production\dump-fdb-destatis.json
fexp -e logs/dump-fdb-destatis.err < P:\eva-dump-configs\production\fexp-jobs\eva-dump-fdb-destatis.fx
java -jar eva-data.jar -clean P:\eva-dump-configs\production\dump-fdb-destatis.json
. /p/workspace/projects/eva-data/scripts/remove_duplicated_kv.sh out/ACC_FDB/ACC_FDB_AVK_FDB_T_Arzt_GOP.csv
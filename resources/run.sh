adbConfig=""
fdbConfig=""
spkConfig=""

java -jar eva-data.jar $adbConfig makejob &&
	fexp < -e /logs/dump-adb.err < eva-dump-adb.fx &&
	java -jar eva-data.jar $adbConfig &
	
java -jar eva-data.jar $fdbConfig makejob &&
	fexp < -e /logs/dump-fdb.err < eva-dump-fdb.fx &&
	java -jar eva-data.jar $fdbConfig &
	
java -jar eva-data.jar $spkConfig makejob &&
	fexp < -e /logs/dump-spk.err < eva-dump-spk.fx &&
	java -jar eva-data.jar $spkConfig &&
	java -jar eva-data.jar $spkConfig map &
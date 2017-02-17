.logtable sb_hri.mw_fexp_log;
.logon 192.168.146.1/$USER,$PASSWORD;
database $DATABASE;
.begin export sessions $SESSIONS;
$TASKS
.logoff;
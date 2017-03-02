# Purpose
This application is used to fetch all data from the teradata data warehouse.

To this end, FastExport is utilized to improve dumping performance.

In a second step data parts are validated and combined into a single CSV file.

## Usage

### 1. Create FastExport script (optional)
Call 

        `java -jar eva-data.jar <config>.json makejob`

to create a FastExport script.

### 2. Run FastExport
Call

        `fexp < <fastExportJob>.fx`

on a shell to start data dumping.

## Config.json file fields
* `server`: server name or IP
    * Example: `127.0.0.1`
* `url`: is the JDBC database type string
    * Example: `jdbc:teradata://`
* `parameters`: Teradata connection parameters. *optional*
    * Example: `CHARSET=UTF8`
* `username`: database user
* `userpassword`: password of the user
* `outputdirectory`: relative directory where the data is saved into. This directory must exist prior execution.
    * Example: `'out'` would specify a directory that lies in the same directory where the jar was executed.
* `tempdirectory`: intermediate files are placed here
* `threads`: determine the number of used threads. Only numbers equal to or larger than 1 are valid. Defaults to `1`.
* `fastexport`: configuration of Teradata's FastExport tool
    * `logDatabase`: database where the log table should be created
    * `logTable`: name of the log table
    * `sessions`: number of sessions used to retrieve data
    * `rowPrefix`: identifies start of a row
    * `jobFilename`: name of the produced FastExport script. Defaults to `fejob.fx`. *optional*
    * `postDumpAction`: shell command that is run after dumping has completed
        * Example: `java -jar absolute/path/to/<jar>.jar <config>.json`
        * Example 2: `java -jar ./<jar>.jar <config>.json`
* `databases`: contains global year filter and database sources
    * `startYear`: earliest year that should be fetched
    * `endYear`: latest year that should be fetched
    * `sources`: array of database sources
        * `name`: name of the database
        * `views`: array of table/view names that are fetched
        * `conditions`: object whose field names denote column names. Queried values are given as an array. Values are OR-ed, columns are AND-ed.
            * Example: `"conditions":{ "h2ik":["7473898", "7474839"] }`

### Full example
```json
{
	"server":"192.168.146.1",
	"url":"jdbc:teradata://",
	"parameters":"TYPE=FASTEXPORT,CHARSET=UTF8",
	"username":"user",
	"userpassword":"password",
	"outputdirectory":"out",
	"tempdirectory":"tmp",
	"threads" : "2",
	"fastexport":{
		"logDatabase": "log_db",
		"logTable" : "log_table",
		"sessions": "24",
		"rowPrefix" : "ROW_START;",
        "jobFilename": "fejob.fx"
	},
	"databases": {
		"startYear":"2010",
		"endYear":"2016",
		"sources":[
				{
					"name":"ACC_ADB",
					"views": [
						"AVK_ADB_T_Arzt_GOP"
					]
				},
				{
					"name":"ACC_FDB",
					"views": [
						"AVK_FDB_T_Arzt_GOP"
					]
				}
			]
		}
}
```
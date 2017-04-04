# Purpose
This application is used to fetch all data from the teradata data warehouse.

To this end, FastExport is utilized to improve dumping performance.

In a second step data parts are validated and combined into a single CSV file.

## Usage

### 1. Create database schema file 
Call

        `java -jar eva-data.jar <config.json> fetchschema`

to create a json representation of the database.

### 2. Create FastExport script
Call 

        `java -jar eva-data.jar <config>.json makejob`

to create a FastExport script.

### 3. Run FastExport
Call

        `fexp < <fastExportJob>.fx`

on a shell to start data dumping.

### 4. Append id mapping
Call

        `java -jar eva-data.jar <config.json> map`

to add an alternative id.

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
* `schemafile`: file to which the database scheme is written
* `fastexport` **_object_**: configuration of Teradata's FastExport tool
    * `logDatabase`: database where the log table should be created
    * `logTable`: name of the log table
    * `sessions`: number of sessions used to retrieve data
    * `rowPrefix`: identifies start of a row
    * `jobFilename`: name of the produced FastExport script. Defaults to `fejob.fx`. *optional*
    * `postDumpAction`: shell command that is run after dumping has completed
        * Example: `java -jar absolute/path/to/<jar>.jar <config>.json`
        * Example 2: `java -jar ./<jar>.jar <config>.json`
* `headerFile`: json file which contains a db -> table -> column(s) mapping. Can be identical with `schemafile`.
* `databases` **_object_**: contains global year filter and database sources
    * `startYear`: earliest year that should be fetched
    * `endYear`: latest year that should be fetched. *optional*
    * `numberOfPreviousYears`: defines that number of years before the current year that are fetched. E.g. if `numberOfPreviousYears` is set to 3 and the current year is 2017 then the years 2014, 2015, 2016 and 2017 are fetched. This property excludes `startYear` and `endYear`.
    * `sources` **_array(object)_**: representing database sources
        * `name`: name of the database
        * `views`: array of table/view **_object_** s that are fetched. The only key is the view/table name
            * `column` **_array(string)_** *optional*: column names. If omitted all columns are used.
            * `where` **_object_** *optional*: keys correspond to restricted columns.
                * `columnName` **_array(object)_**: column restrictions. Entries are OR-ed
                    * `value`: value that this column is compared against
                    * `type`: `NUMERIC`, `STRING` or `COLUMN`. `NUMERIC` indicates that the `value` term is not further modified (like adding apostrophies).
                    * `operator`: SQL comparing symbol
            * `join` **array(object)** *optional*: join clause for a query
                * Each join object has the following fields
                    * `table`: name of the table that appears in the join clause
                    * `type`: type of the join
                    * `column` **array(string)** *optional*: column names that should be selected. If omitted none are taken. If '*' is specified all are taken.
                    * `on`: name of the matched column
                    * `where` *optional*: as above
        * `where` **_object_**: object whose field names denote column names. Queried values are given as an array. Values are OR-ed, columns are AND-ed. Restrictions are imposed on all tables.
            * Example: `"where":{ "h2ik":["7473898", "7474839"] }`
* `mappings` **_array(object)_** *optional*: defines id mapping tasks 
  * `source`: path to the id mapping
  * `sourceKeyColumn`: column in the data file whose entries should be mapped
  * `targetKeyColumn`: column name for the appended ids
  * `targets` **_array(object)_**: files that can be mapped against the same mapping file
    * `data`: path to the unmapped data file
    * `header`: path to the header file for the data

### Option Nodes
#### Where object
```json
{
    ...,
    "where" :{
        "columnName":[
            {"value": "10", "type":"NUMERIC", "operator":"="}
        ]
    }
}
```

## Full example
```json
{
	"server":"192.168.146.1",
	"url":"jdbc:teradata://",
	"parameters":"TYPE=FASTEXPORT,CHARSET=UTF8",
	"username":"user",
	"userpassword":"password",
	"outputdirectory":"out",
	"tempdirectory":"tmp",
	"threads" : 2,
	"schemafile":"headerlookup.json",
	"fastexport":{
		"logDatabase": "sb_hri",
		"logTable" : "your_log_table_name",
		"sessions": 24,
		"rowPrefix" : "ROW_START;",
		"postDumpAction": "java -jar ./eva-data.jar devconfig.json",
		"jobFilename": "headerTest.json"
	},
	"headerFile":"headerlookup.json",
	"databases": {
		"startYear":2010,
		"endYear":2016,
    "numberOfPreviousYears": 3,
		"sources":[
				{
					"name":"ACC_ADB",
					"views": [
						{ "AVK_ADB_T_Arzt_GOP": {
							"column":["PID"],
							"where": {
							"Multiplikator":[{"1":"", "type":"NUMERIC", "operator":"="}]
						},
						"join":[
							{
								"table":"tab",
								"type":"inner",
								"column":["*"],
								"on": "columnname",
								"where":{
									"column":[
									{"value":"38291", "type":"STRING", "operator":"="},
									{"value":"C50", "type":"STRING", "operator":"like"}
									]
								}
							}
						]	
						}
					}
					],					
					"where":{
						"h2ik":[{"value":"kv_h2ik", "type":"STRING", "operator":"="}]
					}
				},
				{
					"name":"ACC_FDB",
					"views": [
						{"AVK_FDB_T_Arzt_GOP":{}}
					]
				}
			]
		},
    "mappings": [
      {
        "source": "path/to/map.csv",
        "targets": [
          { "data": "path/to/data/file", "header": "path/to/header"},
          { "data": "path/to/data/file2", "header": "path/to/header2"}
        ],
        "sourceKeyColumn": "egk_nr",
        "targetKeyColumn": "pid"
      }
	]
}
```
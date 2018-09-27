# Purpose
This application is used to fetch all data from the teradata data warehouse.

To this end, FastExport is utilized to improve dumping performance.

In a second step data parts are validated and combined into a single CSV file.

## Usage

### 1. Create database schema file 
Call

        `java -jar eva-data.jar -fetchschema <config.json>`

to create a json representation of the database.

### 2. Create FastExport scripts
Call 

        `java -jar eva-data.jar -makejob <config>.json`

to create the FastExport scripts.

### 3. Run FastExport
Call

        `java -jar eva-data.jar -dump <config>.json`

to start data dumping using FastExport.

### 4. Clean data
Call

        `java -jar eva-data.jar -clean <config>.json`

to start data cleaning for removal of unwanted characters.

### 5. Merge data
Call

        `java -jar eva-data.jar -merge <config>.json`

to merge slices into a single file.

### 6. Append id mapping
Call

        `java -jar eva-data.jar -map <config.json>`

to add an alternative id. This was intended for SPECTRUM-DB tables but will be replaced by a more generic `append` command.

### 7. Create decoding file
Call

      `java -jar eva-data.jar -makedecode <config.json>`

to create a mapping file containing egk, h2ik, kv number and pid.

### 8. Separate ADB insurances into distinct data sets
Call

      `java -jar eva-data.jar -separate <config.json>`

to extract health insurances from mixed ADB files.

### 9. Append additional data to files
Call

    `java -jar eva-data.jar -append <config.json>`

to execute specified append tasks.


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
                    * `on` **_array(string)_**: name of the matched column
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
* `decoding` **_array(object)_**: each entry denotes an health insurance for which a egk2pid mapping must be created
  * `name`: name of the health insurance
  * `h2ik`: identifier of the health insurance
* `validation` **_array(object)_**: validation rules for data cleaning
  * `rule`: `TYPE` or `NAME`. rule type
  * `type`: `BLOB`,`VARBYTE`,`CHARACTER`,`CLOB`,`VARCHAR`,`DECIMAL`,`DATE`,`FLOAT`,`INTEGER`,`BYTEINT`,`SMALLINT`,`BIGINT`,`NUMBER`,`ANY`,`UNKNOWN`. column type
  * `exclude`: Java regular expression to match unwanted characters
  * `replace` *optional*: String that is used to replace matched characters
  * `column`: name of the column to which the rule is applied
* `separation` **__array(object)__**: datasets that should be distinguished in ADB files
  * `dataset`: name of the dataset
  * `h2iks` **__array(string)__**: one or more ik values associated to the dataset name
* `append` **_array(object)_**: allows to modify files after export
  * `mode`: specifies the append mode. Values: `STATIC`, `DYNAMIC`
  * `match`: filenames must contain this string
  * `targets`: path to files that should be modified
  * `order`: determines whether new column(s) is (are) appended at the beginning or end

  _Only if `mode=STATIC`_
  * `targetColumn`: name of the new column
  * `value`: inserted value
  
  _Only if `mode=MAP`_

  * `source`: path to the file containing additional columns. The file is expected to have a header containing the column names. The `keyColumn` must be the first column. Entries must be semicolon separated.
  * `keyColumn`: column that is used to find rows that belong to the same person. `keyColumn` must exist in both the source and target file.

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
	],
  "decoding": [
		{ "name": "Bosch", "h2ik": "108036123" },
		{ "name": "Salzgitter", "h2ik": "101922757" }
	],
	"validation": [
		{
			"rule": "TYPE", 
			"type": "FLOAT",
			"exclude": "[^\\dEe.+-]" 
		},
		{
			"rule": "TYPE",
			"type": "ANY",
			"exclude": "[^\\p{Alnum}\\s();,./\\\\\u00e4\u00f6\u00fc\u00df\u00a7\u20ac\\[\\]_-]"
		},
		{
			"rule": "TYPE",
			"type": "CHARACTER",
			"exclude": "\u00f6",
			"replace": "\u00f6"
		},
		{
			"rule": "NAME",
			"column": "FG",
			"exclude": "[^\\d]"
		}
	],
  "separation": [
		{
			"dataset" : "Salzgitter",
			"h2iks" : ["101931440", "102137985", "101922757"]
		},
		{
			"dataset" : "Bosch",
			"h2iks" : ["108036123"]
		}
	],
	"append" : [
		{
			"mode": "STATIC",
			"targets": "out/test/static",
			"match": "stat",
			"targetColumn": "H2IK",
			"value": "999999999",
			"order": "FIRST" 
		},
		{
			"mode": "DYNAMIC",
			"targets": "out/test/dynamic",
			"match": "dyn",
			"source": "out/test/map.csv",
			"keyColumn": "egk_nr",
			"order": "LAST"
		}
	]
}
```
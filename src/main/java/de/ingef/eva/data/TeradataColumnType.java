package de.ingef.eva.data;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Mappings from two-letter type codes to human-readable names
 * Source: {@link http://community.teradata.com/t5/Database/List-of-all-Teradata-Column-Types-with-their-associated-DbTypes/m-p/15399/highlight/true#M6768}
 * @author Martin Wettig
 *
 */
@RequiredArgsConstructor
@Getter
public enum TeradataColumnType {
	BYTE("BYTE"),
	BLOB("BLOB"),
	VARBYTE("VARBYTE"),
	CHARACTER("CHARACTER"),
	CLOB("CLOB"),
	VARCHAR("VARCHAR"),
	DECIMAL("DECIMAL"),
	DATE("DATE"),
	FLOAT("FLOAT"),
	INTEGER("INTEGER"),
	BYTEINT("BYTEINT"),
	SMALLINT("SMALLINT"),
	BIGINT("BIGINT"),
	NUMBER("NUMBER"),
	ANY("ANY"),
	UNKNOWN("UNKNOWN");
	
	private final String label;
	
	/**
	 * Converts Teradata type codes into comprehensive type names
	 * @param code uppercase code, e.g. "BF"
	 * @return corresponding enum value
	 */
	public static TeradataColumnType mapCodeToName(String code) {
		switch(code) {
		case "BF":
			return BYTE;
		case "BO":
			return BLOB;
		case "BV":
			return VARBYTE;
		case "CF":
			return CHARACTER;
		case "CO":
			return CLOB;
		case "CV":
			return VARCHAR;
		case "D":
			return DECIMAL;
		case "DA":
			return DATE;
		case "F":
			return FLOAT;
		case "I":
			return INTEGER;
		case "I1":
			return BYTEINT;
		case "I2":
			return SMALLINT;
		case "I8":
			return BIGINT;
		case "N":
			return NUMBER;
		default:
			return UNKNOWN;	
		}
	}
	
	/**
	 * Converts a type name into type enum
	 * @param name uppercase type name, e.g. "BYTE"
	 * @return corresponding enum value
	 */
	public static TeradataColumnType fromTypeName(String name) {
		switch(name) {
		case "BYTE":
			return BYTE;
		case "BLOB":
			return BLOB;
		case "VARBYTE":
			return VARBYTE;
		case "CHARACTER":
			return CHARACTER;
		case "CLOB":
			return CLOB;
		case "VARCHAR":
			return VARCHAR;
		case "DECIMAL":
			return DECIMAL;
		case "DATE":
			return DATE;
		case "FLOAT":
			return FLOAT;
		case "INTEGER":
			return INTEGER;
		case "BYTEINT":
			return BYTEINT;
		case "SMALLINT":
			return SMALLINT;
		case "BIGINT":
			return BIGINT;
		case "NUMBER":
			return NUMBER;
		default:
			return ANY;	
		}
	}
	/*
	 * 
	 *  

case "DH":
			return INTERVAL_DAY_TO_HOUR;
DM INTERVAL DAY TO MINUTE 

DS INTERVAL DAY TO SECOND

DY INTERVAL DAY 

F FLOAT

HM INTERVAL HOUR TO MINUTE 

HS INTERVAL HOUR TO SECOND

HR INTERVAL HOUR 

I INTEGER

I1 BYTEINT 

I2 SMALLINT

I8 BIGINT 

JN JSON

MI INTERVAL MINUTE 

MO INTERVAL MONTH

MS INTERVAL MINUTE TO SECOND 

N NUMBER

PD PERIOD(DATE) 

PM PERIOD(TIMESTAMP WITH TIME ZONE)

PS PERIOD(TIMESTAMP) 

PT PERIOD(TIME)

PZ PERIOD(TIME WITH TIME ZONE) 

SC INTERVAL SECOND

SZ TIMESTAMP WITH TIME ZONE 

TS TIMESTAMP

TZ TIME WITH TIME ZONE 

UT UDT Type

XM XML 

YM INTERVAL YEAR TO MONTH

YR INTERVAL YEAR 

++ TD_ANYTYPE
	 */
}

package de.ingef.eva.etl;

public abstract class Transformer {		
	public abstract Row transform(Row row);
}

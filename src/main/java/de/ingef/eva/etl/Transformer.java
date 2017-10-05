package de.ingef.eva.etl;

public abstract class Transformer {
	public static class NOPTransformer extends Transformer {

		@Override
		public Row transform(Row row) {
			return row;
		}
		
	}
	
	public abstract Row transform(Row row);
}

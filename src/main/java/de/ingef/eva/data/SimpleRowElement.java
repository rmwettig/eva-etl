package de.ingef.eva.data;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SimpleRowElement implements RowElement {
	
	private final String name;
	private final int index;
	private final TeradataColumnType type;
	private final String content;
	
	private final Object value;

	@Override
	public <T> T asType(Class<T> type) throws ClassCastException {
		if(!type.isInstance(type))
			throw new ClassCastException("Could not convert '" + value.getClass().getTypeName() + "' to '" + type.getTypeName() + "'");
		return type.cast(value);
	}

	

	
	
}

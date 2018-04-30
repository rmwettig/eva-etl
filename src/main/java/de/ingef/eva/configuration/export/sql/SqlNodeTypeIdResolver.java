package de.ingef.eva.configuration.export.sql;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

public class SqlNodeTypeIdResolver implements TypeIdResolver {

	private static Map<Class<?>, String> class2Label = new HashMap<>();
	private static Map<String, Class<?>> label2Class = new HashMap<>();
	private JavaType baseType;
	
	@Override
	public String getDescForKnownTypeIds() {
		return new TreeSet<String>(label2Class.keySet()).toString();
	}

	@Override
	public Id getMechanism() {
		return Id.CUSTOM;
	}

	@Override
	public String idFromBaseType() {
		return "DEFAULT";
	}

	@Override
	public String idFromValue(Object value) {
		return idFromValueAndType(value, value.getClass());
	}

	@Override
	public String idFromValueAndType(Object value, Class<?> suggestedType) {
		String result = class2Label.get(suggestedType);
		if(result == null)
			throw new IllegalStateException("No id for " + suggestedType + " for base " + baseType.getTypeName());
		return result;
	}

	@Override
	public void init(JavaType baseType) {
		this.baseType = baseType;
		initializeMappings(baseType.getRawClass());
	}

	@Override
	public JavaType typeFromId(DatabindContext context, String id) throws IOException {
		Class<?> result = label2Class.get(id);
		if(result == null)
			throw new IllegalStateException("No type " + id + " for " + baseType.getClass() + ". Try: " + getDescForKnownTypeIds());
		return TypeFactory.defaultInstance().constructSpecializedType(baseType, result);
	}
	
	/**
	 * run class path scanning here to obtain class type mappings
	 * @param rawClass
	 */
	private static void initializeMappings(Class<?> rawClass) {
		Set<Class<?>> types = new HashSet<>();
		ScanResult result = new FastClasspathScanner()
				.matchClassesWithAnnotation(JsonTypeName.class, types::add)
				.scan();
		for(Class<?> type : types) {
			JsonTypeName typeName = type.getAnnotation(JsonTypeName.class);
			class2Label.put(type, typeName.value());
			label2Class.put(typeName.value(), type);
		}
	}

}

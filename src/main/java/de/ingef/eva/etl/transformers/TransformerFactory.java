package de.ingef.eva.etl.transformers;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.configuration.append.AppendSourceConfig;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class TransformerFactory {
	
	public List<Transformer> create(Configuration mainConfig, List<AppendConfiguration> appendConfigs) {
		return appendConfigs.stream().map(config -> of(mainConfig, config)).collect(Collectors.toList());
	}
	
	private Transformer of(Configuration mainConfig, AppendConfiguration config) {
		switch (config.getMode()) {
		case STATIC:
			return new StaticColumnAppenderTransformer(
					config.getTargetDb(),
					config.getTargetTable(),
					config.getValueName(),
					config.getValue(),
					config.getOrder(),
					config.getExcludeTables());
		case DDD:
			return createDDDTransformer(config);
		case BS_TO_KV:
			return new BsKvMapperTransformer();
		case APO_TYPE:
			return new PharmacyTypeTransformer(config.getTargetDb(), config.getTargetTable());
		case DYNAMIC:
			return DynamicColumnAppender.of(config);
		case START_DATE:
			return new StartDateTransformer(config.getTargetDb(), config.getTargetTable(), config.getEndDateColumn(), config.getDayColumn(), config.getValueName());
		case FIX_DATES:
			return new FixMissingDateEntries();
		default:
			return new Transformer.NOPTransformer();
		}
	}

	private Transformer createDDDTransformer(AppendConfiguration config) {
		List<AppendSourceConfig> sources = config.getSources();
		String keyColumn = config.getKeyColumn();
		//check mandatory fields
		if(sources == null || sources.isEmpty() || keyColumn == null || keyColumn.isEmpty()) {
			log.warn("Append sources or keyColumn fields are missing or are empty. Using NOPTransformer.");
			return new Transformer.NOPTransformer();
		}
		String db = config.getTargetDb();
		String table = config.getTargetTable();
		//if both db and table are missing use no-op
		if((db == null || db.isEmpty()) && (table == null || table.isEmpty())) {
			log.warn("Append db or target table field is null or empty. Using NOPTransformer");
			return new Transformer.NOPTransformer();
		}
		try {
			return DDDTransformer.of(
					db,
					table,
					keyColumn,
					sources
			);
		} catch (IOException e) {
			log.warn("Could not create DDDTransformer. Using NOPTransformer. {}", e);
			return new Transformer.NOPTransformer();
		}
	}
}

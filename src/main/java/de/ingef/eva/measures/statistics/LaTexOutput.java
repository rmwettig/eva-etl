package de.ingef.eva.measures.statistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;
import de.ingef.eva.utility.latex.Figure;
import de.ingef.eva.utility.latex.LatexNode;
import de.ingef.eva.utility.latex.Literal;
import de.ingef.eva.utility.latex.PreambleEntry;
import de.ingef.eva.utility.latex.TexDocument;
import de.ingef.eva.utility.latex.TextColor;
import de.ingef.eva.utility.latex.TextColor.Color;
import de.ingef.eva.utility.latex.Wrapper;
import de.ingef.eva.utility.latex.alignment.AlignmentOption;
import de.ingef.eva.utility.latex.alignment.Anchor;
import de.ingef.eva.utility.latex.environment.Landscape;
import de.ingef.eva.utility.latex.environment.Table;
import de.ingef.eva.utility.latex.page.PageBreak;
import de.ingef.eva.utility.latex.page.TitlePage;
import de.ingef.eva.utility.latex.table.BorderMode;
import de.ingef.eva.utility.latex.table.EndHead;
import de.ingef.eva.utility.latex.table.HLine;
import de.ingef.eva.utility.latex.table.MultiColumnRow;
import de.ingef.eva.utility.latex.table.RowContent;
import de.ingef.eva.utility.latex.table.Tabular;
import de.ingef.eva.utility.latex.table.Tabular.TabularType;

public class LaTexOutput {
	
	private static final String DOCUMENT_FONT = "Roboto";
	private static final String DOCUMENT_TITLE = "EVA-F\u00fcllstandsbericht";
	private static final String OVERVIEW_CAPTION = "Datenstand der Datenbereiche zum elektronischen Datenaustausch der GKV";
	private static final String DETAIL_CAPTION = "Aktueller Datenstand der ambulanten Daten pro KV";
	private static final String MORBI_CAPTION = "Morbidit\u00e4tsorientierter Risikostrukturausgleich";
	private static final DecimalFormat NUMBER_FORMATTER = (DecimalFormat) NumberFormat.getInstance(Locale.GERMAN);
	private static final String LOGO_PATH = "/resources/ingef_logo.jpg";
	
	static {
		NUMBER_FORMATTER.applyLocalizedPattern("###.###,##");
	}
		
	public void createStatisticTexFile(Path output, List<StatisticsEntry> overviewStatistic, List<StatisticsEntry> detailStatistic, List<MorbiRsaEntry> morbiStatistic, List<String> morbiColumnHeader) {
		TexDocument.TexDocumentBuilder document = createDocument();
		document.element(new TitlePage(new Figure(Anchor.BOTTOM, Paths.get(LOGO_PATH), 0.25f)));
		document.element(new PageBreak());
		document.element(createOverviewTable(overviewStatistic));
		document.element(new PageBreak());
		document.element(createDetailTable(detailStatistic));
		document.element(new PageBreak());
		document.element(createMorbiTable(morbiStatistic, morbiColumnHeader));
		
		writeToFile(output, document.build().renderDocument());
	}
	
	private TexDocument.TexDocumentBuilder createDocument() {
		return TexDocument
				.builder()
				.preambleEntry(PreambleEntry.builder().name("documentclass").option("12pt").option("a4paper").option("twoside").value("article").build())
				.preambleEntry(PreambleEntry.builder().name("usepackage").option("utf8").value("inputenc").build())
				.preambleEntry(PreambleEntry.builder().name("usepackage").value("fontspec").build()) 
				.preambleEntry(PreambleEntry.builder().name("usepackage").value("graphicx").build())
				.preambleEntry(PreambleEntry.builder().name("usepackage").value("float").build())
				.preambleEntry(PreambleEntry.builder().name("usepackage").value("color").build())
				.preambleEntry(PreambleEntry.builder().name("usepackage").value("lscape").build())
				.preambleEntry(PreambleEntry.builder().name("usepackage").value("longtable").build())
				.preambleEntry(PreambleEntry.builder().name("setmainfont").value(DOCUMENT_FONT).build())
				.preambleEntry(PreambleEntry.builder().name("title").value(DOCUMENT_TITLE).build())
				.preambleEntry(PreambleEntry.builder().name("date").value("\\the\\day.\\the\\month.\\the\\year").build());
	}
	
	private LatexNode createOverviewTable(List<StatisticsEntry> overview) {
		int columnCount = overview.size() + 1;
		Tabular.TabularBuilder builder =
				Tabular
					.builder()
					.type(TabularType.LONGTABLE)
					.columnOptions(createCenteredRows(columnCount))
					.row(new MultiColumnRow(columnCount, OVERVIEW_CAPTION, Anchor.CENTER))
					.row(new HLine())
					.row(createOverviewHeader(overview))
					.row(new HLine())
					.row(new EndHead());
					
		createOverviewBody(createQuarterToRowContent(overview)).stream().forEach(row -> builder.row(row));

		return new Landscape(builder.build());
	}
	
	private RowContent createOverviewHeader(List<StatisticsEntry> overview) {
		RowContent.RowContentBuilder builder = 
				RowContent
					.builder()
					.value("Quartal");
		overview
			.stream()
			.map(entry -> entry.getLabel())
			.forEach(label -> builder.value(label));
		return builder.build();
	}
	
	private List<RowContent> createOverviewBody(Map<Quarter, List<QuarterCount>> rows) {
		int rowIndexCounter = 0;
		List<RowContent> rowContent = new ArrayList<>(rows.size());
		for(Map.Entry<Quarter, List<QuarterCount>> entry : rows.entrySet()) {
			RowContent.RowContentBuilder builder = RowContent.builder();
			Quarter quarter = entry.getKey();
			builder.value(quarter.getYear() + "Q" + quarter.getQuarter());
			int rowIndex = rowIndexCounter++;
			entry.getValue()
				.stream()
				.map(quarterCount -> convertQuarterCountToTableData(quarterCount, rowIndex))
				.forEach(value -> builder.value(value));
			rowContent.add(builder.build());
		}
		
		return rowContent;
	}
	
	private LatexNode createDetailTable(List<StatisticsEntry> detailStatistic) {
		Tabular.TabularBuilder builder =
				Tabular
					.builder()
					.type(TabularType.TABULAR)
					.columnOptions(createCenteredRows(5))
					.row(new MultiColumnRow(5, DETAIL_CAPTION, Anchor.CENTER))
					.row(new HLine())
					.row(createDetailHeader())
					.row(new HLine());
		
		createDetailBody(detailStatistic).stream().forEach(content -> builder.row(content));
		
		return new Table(Anchor.HERE, builder.build());
	}
	
	private RowContent createDetailHeader() {
		return RowContent
				.builder()
				.value("Quartal")
				.value("KV")
				.value("KV-Name")
				.value("Anzahl F\u00e4lle")
				.value("Anteil VJQ")
				.build();
	}
	
	private List<RowContent> createDetailBody(List<StatisticsEntry> rows) {
		return rows
				.stream()
				.map(this::convertToRow)
				.collect(Collectors.toList());
	}
	
	private LatexNode createMorbiTable(List<MorbiRsaEntry> morbiStatistic, List<String> morbiColumnHeader) {
		Tabular.TabularBuilder builder =
				Tabular
					.builder()
					.type(TabularType.TABULAR)
					.columnOptions(createCenteredRows(5))
					.row(new MultiColumnRow(5, MORBI_CAPTION, Anchor.CENTER))
					.row(new HLine())
					.row(createMorbiHeader(morbiColumnHeader))
					.row(new HLine());
		
		createMorbiBody(morbiStatistic).stream().forEach(row -> builder.row(row));
		return new Table(Anchor.HERE, builder.build());
	}
	
	private RowContent createMorbiHeader(List<String> morbiColumnHeader) {
		RowContent.RowContentBuilder builder = RowContent.builder();
		morbiColumnHeader.stream().forEach(header -> builder.value(header));
		return builder.build();
	}
	
	private List<RowContent> createMorbiBody(List<MorbiRsaEntry> morbiStatistic) {
		return morbiStatistic
				.stream()
				.map(this::convertMorbiEntryToRow)
				.collect(Collectors.toList());
	}
	
	private RowContent convertMorbiEntryToRow(MorbiRsaEntry entry) {
		return RowContent
				.builder()
				.value(Integer.toString(entry.getGrouperYear()))
				.value(Integer.toString(entry.getBenefitYear()))
				.value(Integer.toString(entry.getReportingYear()))
				.value(entry.getQuarter())
				.value(entry.getConfiguration())
				.build();
	}
	
	private RowContent convertToRow(StatisticsEntry entry) {	
		QuarterCount qc = entry.getDataCount().get(0);
		Quarter q = qc.getQuarter();
		return RowContent
				.builder()
				.value(q.getYear() + "Q" + q.getQuarter())
				.value(entry.getIdentifier())
				.value(entry.getLabel())
				.value(NUMBER_FORMATTER.format(qc.getCount()))
				.value(createRatioText(qc.getChangeRatio()).render())
				.build();
	}
	
	private List<AlignmentOption> createCenteredRows(int count) {
		return IntStream
				.range(0, count)
				.mapToObj(i -> i == 0 ? new AlignmentOption(Anchor.CENTER, BorderMode.RIGHT) : new AlignmentOption(Anchor.CENTER, BorderMode.NONE))
				.collect(Collectors.toList());
	}
	
	private Map<Quarter, List<QuarterCount>> createQuarterToRowContent(List<StatisticsEntry> entries) {
		//use a linked hash map to preserve order
		Map<Quarter, List<QuarterCount>> table = new LinkedHashMap<>(entries.size());
		entries
			.stream()
			.flatMap(entry -> entry.getDataCount().stream())
			.forEachOrdered(quarterCount -> {
				if(!table.containsKey(quarterCount.getQuarter())) {
					List<QuarterCount> content = new ArrayList<>(100);
					content.add(quarterCount);
					table.put(quarterCount.getQuarter(), content);
				} else {
					table.get(quarterCount.getQuarter()).add(quarterCount);
				}
			});
		return table;
	}
	
	private String convertQuarterCountToTableData(QuarterCount quarterCount, int rowIndex) {
		StringBuilder sb = new StringBuilder();
		sb.append(NUMBER_FORMATTER.format(quarterCount.getCount()));
		double ratio = quarterCount.getChangeRatio();
		//do not highlight first four quarters as these have no preceding quarters to compare with
		if(rowIndex < 4)
			return sb.toString();
		
		sb.append(createRatioText(ratio).render());
		
		return sb.toString();
	}

	private LatexNode createRatioText(double ratio) {
		//deviation outside of tolerance thresholds
		if(ratio > 1.5 || ratio < 0.5)
			return new Wrapper("(", ")", new TextColor(Color.RED, NUMBER_FORMATTER.format(ratio)));
		else
			return new Wrapper("(", ")", new Literal(NUMBER_FORMATTER.format(ratio)));
	}
	
	private void writeToFile(Path outputFile, String output) {
		try (
			BufferedWriter writer = Files.newBufferedWriter(outputFile, OutputDirectory.DATA_CHARSET);
		) {
			writer.write(output);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

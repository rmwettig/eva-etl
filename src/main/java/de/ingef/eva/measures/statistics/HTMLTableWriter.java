package de.ingef.eva.measures.statistics;

import static j2html.TagCreator.caption;
import static j2html.TagCreator.head;
import static j2html.TagCreator.html;
import static j2html.TagCreator.meta;
import static j2html.TagCreator.style;
import static j2html.TagCreator.table;
import static j2html.TagCreator.tbody;
import static j2html.TagCreator.td;
import static j2html.TagCreator.th;
import static j2html.TagCreator.tr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;
import j2html.tags.ContainerTag;

public class HTMLTableWriter {

	private static final ContainerTag GLOBAL_TABLE_STYLES = style("table { margin-top: 100px; border-collapse: collapse; margin-left: auto; margin-right: auto; border: black solid 1px; }");
	private static final String OVERVIEW_CAPTION = "Datenstand der Datenbereiche zum elektronischen Datenaustausch der GKV";
	private static final String DETAIL_CAPTION = "Aktueller Datenstand der ambulanten Daten pro KV";	
	private static final DecimalFormat NUMBER_FORMATTER = (DecimalFormat) NumberFormat.getInstance(Locale.GERMAN);
	
	public void createStatisticHTMLFile(Path output, List<StatisticsEntry> overviewStatistic, List<StatisticsEntry> detailStatistic) {
		ContainerTag overviewTable = createOverviewTable(overviewStatistic);
		ContainerTag detailsTable = createDetailTable(detailStatistic);
		ContainerTag htmlDocument = createHtmlDocument();
		htmlDocument.with(overviewTable);
		htmlDocument.with(detailsTable);
		
		writeToFile(output, htmlDocument.render());
	}
	
	private ContainerTag createOverviewTable(List<StatisticsEntry> overviewStatistic) {
		ContainerTag header = createOverviewHeader(overviewStatistic);
		Map<Quarter,List<QuarterCount>> transposedData = createQuarterToRowContent(overviewStatistic);
		ContainerTag tableBody = createOverviewBody(transposedData);
		ContainerTag table = createHtmlTableStub(OVERVIEW_CAPTION, GLOBAL_TABLE_STYLES, header);
		return table.with(tableBody);
	}

	private ContainerTag createDetailTable(List<StatisticsEntry> detailStatistic) {
		ContainerTag header = createDetailHeader();
		ContainerTag tableBody = createDetailBody(detailStatistic);
		return createHtmlTableStub(DETAIL_CAPTION, GLOBAL_TABLE_STYLES, header).with(tableBody);
	}
	
	private ContainerTag createOverviewHeader(List<StatisticsEntry> entries) {
		ContainerTag headerRow = tr();
		headerRow.with(th().withText("Quartal"));
		entries
			.stream()
			.map(entry -> entry.getLabel())
			.forEachOrdered(label -> headerRow.with(th().withText(label)));
		return headerRow;
	}
	
	private ContainerTag createDetailHeader() {
		return tr()
				.with(th().withText("Quartal"))
				.with(th().withText("KV"))
				.with(th().withText("KV_Name"))
				.with(th().withText("Anzahl Fälle"))
				.with(th().withText("Anteil VJQ"));
	}
	
	private Map<Quarter,List<QuarterCount>> createQuarterToRowContent(List<StatisticsEntry> entries) {
		//use a linked hash map to preserve order
		Map<Quarter,List<QuarterCount>> table = new LinkedHashMap<>(entries.size());
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
	
	private ContainerTag createOverviewBody(Map<Quarter,List<QuarterCount>> rows) {
		ContainerTag tableBody = tbody();
		rows
			.forEach((quarter, data) -> {
				String quarterLabel = quarter.getYear() + "Q" + quarter.getQuarter();
				ContainerTag row = tr().with(td().withText(quarterLabel));
				List<ContainerTag> tableCells = data
					.stream()
					.map(this::convertQuarterCountToTableData)
					.collect(Collectors.toList());
				tableCells.stream().forEachOrdered(cell -> row.with(cell));
				tableBody.with(row);
			});
		return tableBody;
	}
	
	private ContainerTag createDetailBody(List<StatisticsEntry> rows) {
		List<ContainerTag> tableRows = rows
				.stream()
				.map(this::convertToRow)
				.collect(Collectors.toList());
		ContainerTag tableBody = tbody();
		tableRows.stream().forEachOrdered(row -> tableBody.with(row));
		return tableBody;
	}
	
	private ContainerTag convertQuarterCountToTableData(QuarterCount quarterCount) {
		StringBuilder sb = new StringBuilder();
		sb.append(NUMBER_FORMATTER.format(quarterCount.getCount()));
		ContainerTag cell = td();
		double ratio = quarterCount.getChangeRatio();
		if(ratio > 0)
			sb.append(" (" + NUMBER_FORMATTER.format(ratio) + ")");
		if(ratio > 1.5 || ratio < 0.5)
			cell.withClass("alertRatio");
		
		return cell.withText(sb.toString());
	}
	
	private ContainerTag convertToRow(StatisticsEntry entry) {
		ContainerTag row = tr();		
		QuarterCount qc = entry.getDataCount().get(0);
		Quarter q = qc.getQuarter();
		row.with(td().withText(q.getYear() + "Q" + q.getQuarter()));
		row.with(td().withText(entry.getIdentifier()));
		row.with(td().withText(entry.getLabel()));
		row.with(td().withText(NUMBER_FORMATTER.format(qc.getCount())));
		double ratio = qc.getChangeRatio();
		row.with(createRatioCell(ratio));
				
		return row;
	}
	
	private ContainerTag createRatioCell(double ratio) {
		ContainerTag ratioCell = td();
		if(ratio > 1.5 || ratio < 0.5)
			ratioCell.withClass("alertRatio");
		ratioCell.withText(NUMBER_FORMATTER.format(ratio));
		return ratioCell;
	}
	
	private ContainerTag createHtmlTableStub(String caption, ContainerTag styles, ContainerTag header) {
		ContainerTag htmlTable = table();
		htmlTable.with(GLOBAL_TABLE_STYLES);
		htmlTable.with(caption().withText(caption));
		htmlTable.with(header);
		
		return htmlTable;
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
	
	/**
	 * creates an html document with global styles
	 * @return
	 */
	private ContainerTag createHtmlDocument() {
		String cssStyles = "td { text-align: center; padding: 13px; box-sizing: border-box; }";
		cssStyles += " tr:nth-child(odd){ background-color: lightgray; }";
		cssStyles += " caption { font-size: 24px; font-weight: bold; margin-bottom: 10px; }";
		cssStyles += "th:nth-child(1) { border-right: 1px solid black; }";
		cssStyles += "td:nth-child(1) { border-right: 1px solid black; }";
		cssStyles += "th { border-bottom: 1px solid black; background-color: white; }";
		cssStyles += ".alertRatio { color: red; font-weight: bold; }";
		
		ContainerTag document = html();
		document.with(head().with(meta().withCharset("utf-8").withContent("text/html")));
		document.with(style(cssStyles));
		
		return document;
	}
}

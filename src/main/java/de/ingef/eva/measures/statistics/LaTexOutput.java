package de.ingef.eva.measures.statistics;

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
import java.util.stream.IntStream;

import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;

public class LaTexOutput {
	
	private static final String OVERVIEW_CAPTION = "Datenstand der Datenbereiche zum elektronischen Datenaustausch der GKV";
	private static final String DETAIL_CAPTION = "Aktueller Datenstand der ambulanten Daten pro KV";
	private static final String MORBI_CAPTION = "Morbidit\u00e4tsorientierter Risikostrukturausgleich";
	private static final DecimalFormat NUMBER_FORMATTER = (DecimalFormat) NumberFormat.getInstance(Locale.GERMAN);
	//private static final String FONT_PATH = "/fonts/Roboto/Roboto-Regular.ttf";
	//private static final String LOGO_PATH = "/logos/INGEF_Logo_ohne_claim.jpg";
	
	static {
		NUMBER_FORMATTER.applyLocalizedPattern("###.###,##");
	}
	
	private static String preamble = "\\documentclass[12pt, a4paper, twoside]{article}\n" + 
			"\\usepackage[utf8]{inputenc}\n" + 
			"\\usepackage{fontspec}\n" + 
			"\\usepackage{graphicx}\n" + 
			"\\usepackage{float}\n" + 
			"\\usepackage{color}\n" +
			"\\usepackage{lscape}\n" +
			"\\usepackage{longtable}\n" +
			"\n" + 
			"\\setmainfont{Roboto}\n" + 
			"\\title{EVA-F\u00fcllstandsbericht}\n" + 
			"\\date{\\the\\day.\\the\\month.\\the\\year}\n";
		
	public void createStatisticHTMLFile(Path output, List<StatisticsEntry> overviewStatistic, List<StatisticsEntry> detailStatistic, List<MorbiRsaEntry> morbiStatistic, List<String> morbiColumnHeader) {
		StringBuilder document = createDocument();
		setPreamble(document);
		openDocument(document);
		insertTitlePage(document, "");
		insertPage(document);
		document.append(createOverviewTable(overviewStatistic));
		insertPage(document);
		document.append(createDetailTable(detailStatistic));
		insertPage(document);
		document.append(createMorbiTable(morbiStatistic, morbiColumnHeader));
		closeDocument(document);
		writeToFile(output, document.toString());
	}
	
	private StringBuilder createDocument() {
		return new StringBuilder();
	}
	
	private void openDocument(StringBuilder document) {
		document.append("\\begin{document}\n");
	}
	
	private void closeDocument(StringBuilder document) {
		document.append("\\end{document}");
	}
	
	private void setPreamble(StringBuilder document) {
		document.append(preamble);
	}
	
	private void insertPage(StringBuilder document) {
		document.append("\\newpage\n");
	}
	
	private void insertTitlePage(StringBuilder document, String imagePath) {
		document.append("\\begin{titlepage}\n");
		document.append("\\maketitle\n");
		//FIXME enable this later on to get the company logo
		//document.append(insertLogo(imagePath));
		document.append("\\end{titlepage}\n");
	}
	
	private String insertLogo(String image) {
		StringBuilder figure = new StringBuilder();
		figure.append("\\begin{figure}[b]\n");
		figure.append("\\includegraphics[scale=0.25]{" + image + "}\n");
		figure.append("\\end{figure}\n");
		
		return figure.toString();
	}
	
	private String addTableEnvironment(String tabular) {
		return "\\begin{table}[h]\n" +
				tabular +
				"\\end{table}\n";
	}
	
	private String addLongTable(String content, String alignment) {
		return "\\begin{longtable}{" + alignment + "}\n" +
				content + 
				"\\end{longtable}";
	}
	
	private String useLandscape(String content) {
		return "\\begin{landscape}\n" +
				content +
				"\\end{landscape}\n";
	}
	
	private String createOverviewTable(List<StatisticsEntry> overview) {
		StringBuilder tabular = new StringBuilder();
		int columnCount = overview.size() + 1;
		tabular.append("\\multicolumn{" + columnCount + "}{c}{" + OVERVIEW_CAPTION + "} \\\\\n");
		tabular.append("\\hline\n");
		tabular.append(createOverviewHeader(overview) + "\\\\\n");
		tabular.append("\\hline\n");
		tabular.append("\\endhead\n");
		tabular.append(createOverviewBody(createQuarterToRowContent(overview)));
		return useLandscape(addLongTable(tabular.toString(), createCenteredRows(columnCount)));
	}
	
	private String createOverviewHeader(List<StatisticsEntry> overview) {
		return "Quartal & " + overview
				.stream()
				.map(entry -> entry.getLabel())
				.collect(Collectors.joining(" & "));
	}
	
	private String createOverviewBody(Map<Quarter, List<QuarterCount>> rows) {
		StringBuilder body = new StringBuilder();
		int rowIndexCounter = 0;
		for(Map.Entry<Quarter, List<QuarterCount>> entry : rows.entrySet()) {
			Quarter quarter = entry.getKey();
			StringBuilder row = new StringBuilder();
			row.append(quarter.getYear() + "Q" + quarter.getQuarter() + " & ");
			int rowIndex = rowIndexCounter++;
			String tableCells = entry.getValue()
				.stream()
				.map(quarterCount -> convertQuarterCountToTableData(quarterCount, rowIndex))
				.collect(Collectors.joining(" & "));
			row.append(tableCells);
			body.append(row  + "\\\\\n" );
		}
		//remove new table row sign after last row
		body.delete(body.lastIndexOf("\\\\\n"), body.length());
		return body.toString();
	}
	
	private String createDetailTable(List<StatisticsEntry> detailStatistic) {
		StringBuilder tabular = new StringBuilder();
		tabular.append("\\begin{tabular}{" + createCenteredRows(5) + "}\n");
		tabular.append("\\multicolumn{5}{c}{" + DETAIL_CAPTION + "} \\\\\n");
		tabular.append("\\hline\n");
		tabular.append(createDetailHeader() + "\\\\\n");
		tabular.append("\\hline\n");
		tabular.append(createDetailBody(detailStatistic));
		tabular.append("\n\\end{tabular}\n");
		return addTableEnvironment(tabular.toString());
	}
	
	private String createDetailHeader() {
		StringBuilder header = new StringBuilder();
		header.append("Quartal & ");
		header.append("KV &");
		header.append("KV-Name & ");
		header.append("Anzahl F\u00e4lle &");
		header.append("Anteil VJQ \\\\");
		return header.toString();
	}
	
	private String createDetailBody(List<StatisticsEntry> rows) {
		return rows
				.stream()
				.map(this::convertToRow)
				.collect(Collectors.joining(" \\\\\n"));
	}
	
	private String createMorbiTable(List<MorbiRsaEntry> morbiStatistic, List<String> morbiColumnHeader) {
		StringBuilder tabular = new StringBuilder();
		tabular.append("\\begin{tabular}{" + createCenteredRows(5) + "}\n");
		tabular.append("\\multicolumn{5}{c}{" + MORBI_CAPTION + "} \\\\\n");
		tabular.append("\\hline\n");
		tabular.append(createMorbiHeader(morbiColumnHeader) + "\\\\\n");
		tabular.append("\\hline\n");
		tabular.append(createMorbiBody(morbiStatistic));
		tabular.append("\n\\end{tabular}\n");
		return addTableEnvironment(tabular.toString());
	}
	
	private String createMorbiHeader(List<String> morbiColumnHeader) {
		return morbiColumnHeader
				.stream()
				.collect(Collectors.joining(" & "));
	}
	
	private String createMorbiBody(List<MorbiRsaEntry> morbiStatistic) {
		return morbiStatistic
				.stream()
				.map(this::convertMorbiEntryToRow)
				.collect(Collectors.joining(" \\\\\n"));
	}
	
	private String convertMorbiEntryToRow(MorbiRsaEntry entry) {
		StringBuilder row = new StringBuilder();
		row.append(Integer.toString(entry.getGrouperYear()) + " & ");
		row.append(Integer.toString(entry.getBenefitYear()) + " & ");
		row.append(Integer.toString(entry.getReportingYear()) + " & ");
		row.append(entry.getQuarter() + " & ");
		row.append(entry.getConfiguration());
		return row.toString();
	}
	
	private String convertToRow(StatisticsEntry entry) {
		StringBuilder row = new StringBuilder();		
		QuarterCount qc = entry.getDataCount().get(0);
		Quarter q = qc.getQuarter();
		row.append(q.getYear() + "Q" + q.getQuarter() + " & ");
		row.append(entry.getIdentifier() + " & ");
		row.append(entry.getLabel() + " & ");
		row.append(NUMBER_FORMATTER.format(qc.getCount()) + " & ");
		row.append(createRatioText(qc.getChangeRatio()));
		
		return row.toString();
	}
	
	private String createCenteredRows(int count) {
		return IntStream
				.range(0, count)
				.mapToObj(i -> i == 0 ? "c |" : "c")
				.collect(Collectors.joining(" "));
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
		
		sb.append(createRatioText(ratio));
		
		return sb.toString();
	}

	private String createRatioText(double ratio) {
		//deviation outside of tolerance thresholds
		if(ratio > 1.5 || ratio < 0.5)
			return " (\\textcolor{red}{" + NUMBER_FORMATTER.format(ratio) + "})";
		else
			return " (" + NUMBER_FORMATTER.format(ratio) + ")";
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

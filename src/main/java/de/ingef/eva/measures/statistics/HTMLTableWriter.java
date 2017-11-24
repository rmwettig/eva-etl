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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.file.FileDataTable;
import de.ingef.eva.error.DataTableOperationException;
import j2html.tags.ContainerTag;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class HTMLTableWriter implements DataProcessor {

	private final String outputDirectory;
	private final String file;
	private final String caption;
	private Map<String, ContainerTag[]> quarterToHtmlData = new LinkedHashMap<>();
	private ContainerTag globalTableStyles = style("table { margin-top: 100px; border-collapse: collapse; margin-left: auto; margin-right: auto; border: black solid 1px; }");
		
	@Override
	public DataTable process(DataTable... dataTables) {
		ContainerTag headerRow = tr();
		headerRow.with(th().withText("Quartal"));
		//last entry is the detail overview
		for(int i = 0; i < dataTables.length - 1; i++) {
			DataTable table = dataTables[i];
			headerRow.with(th().withText(table.getName()));
			try {
				table.open();
				Pattern alertPattern = Pattern.compile("\\(0,[0-8]{1}\\d*\\)|\\([0-9]{2,},\\d*\\)|\\([2-9]{1},\\d*\\)|\\(1,[2-9]{1}\\d*\\)"); 
				while(table.hasMoreRows()) {
					List<RowElement> row = table.getNextRow(true);
					String quarter = row.get(0).getContent();
					if(quarterToHtmlData.containsKey(quarter)) {
						// create td elements here with offset
						quarterToHtmlData.get(quarter)[i] = rowToHtmlRowElements(row, alertPattern);
					} else {
						ContainerTag[] tableDataEntries = new ContainerTag[dataTables.length - 1];
						tableDataEntries[i] = rowToHtmlRowElements(row, alertPattern);
						quarterToHtmlData.put(quarter, tableDataEntries);
					}
				}
				table.close();
			} catch(DataTableOperationException e) {
				log.error(e.getMessage());
			}
		}
		
		//create tables for full data statistics and details
		ContainerTag overview = createHtmlTable(headerRow, quarterToHtmlData);
		ContainerTag details = createDetailsTable(dataTables[dataTables.length - 1], "Aktueller Datenstand der ambulanten Daten pro KV");
		
		ContainerTag htmlDocument = createHtmlDocument();
		htmlDocument.with(overview);
		htmlDocument.with(details);
		
		return writeToFile(outputDirectory, file, htmlDocument.render());
	}
	
	/**
	 * assembles table header and body rows
	 * @param header
	 * @param rows
	 * @return html table string
	 */
	private ContainerTag createHtmlTable(ContainerTag header, Map<String, ContainerTag[]> rows) {
		ContainerTag htmlTable = createHtmlTableStub(caption, globalTableStyles, header);
		ContainerTag tableBody = tbody();
		for(String yearQuarter : rows.keySet()) {
			ContainerTag row = tr().with(td().withText(yearQuarter));
			for(ContainerTag tableData : rows.get(yearQuarter)) {
				if(tableData == null) {
					row.with(td().withText("0"));
				} else {
					row.with(tableData);
				}
			}
			tableBody.with(row);
		}
		htmlTable.with(tableBody);
		
		return htmlTable;
	}
	
	private ContainerTag createDetailsTable(DataTable dataTable, String caption) {
		try {
			dataTable.open();
			ContainerTag header = tr();
			dataTable.getColumnNames().forEach(rowElement -> header.with(th().withText(rowElement.getContent())));
			
			ContainerTag htmlTable = createHtmlTableStub(caption, globalTableStyles, header);
			ContainerTag tableBody = tbody();
			Pattern alertPattern = Pattern.compile("0,[0-8]{1}\\d*|[2-9],\\d+|1,[2-9]{1}\\d*");
			while(dataTable.hasMoreRows()) {
				ContainerTag htmlRow = tr();
				for(ContainerTag entry : detailRowToHtmlRowElements(dataTable.getNextRow(true), alertPattern))
					htmlRow.with(entry);
				
				tableBody.with(htmlRow);
			}
			
			htmlTable.with(tableBody);
			return htmlTable;
		} catch (DataTableOperationException e) {
			log.error(e.getMessage());
		}
		
		return null;
	}
	
	private ContainerTag createHtmlTableStub(String caption, ContainerTag styles, ContainerTag header) {
		ContainerTag htmlTable = table();
		htmlTable.with(globalTableStyles);
		htmlTable.with(caption().withText(caption));
		htmlTable.with(header);
		
		return htmlTable;
	}
	
	/**
	 * takes a raw row and creates html table elements
	 * 
	 * Row format example:
	 * 2010Q2 4.322 (0,34)
	 * @param row
	 * @param offset
	 * @return single table data element
	 */
	private ContainerTag rowToHtmlRowElements(List<RowElement> row, Pattern alertPattern) {
		//read from the remaining row array
		String content = row.get(1).getContent();
		ContainerTag element = td().withText(content);
		if(alertPattern.matcher(content).find()) element.withClass("alertRatio");
				
		return element;
	}
	
	private ContainerTag[] detailRowToHtmlRowElements(List<RowElement> row, Pattern alertPattern) {
		int rowSize = row.size();
		ContainerTag[] elements = new ContainerTag[rowSize];
		for(int i = 0; i < elements.length - 1; i++)
			//read from the remaining row array
			elements[i] = td().withText(row.get(i).getContent());
		
		int lastEntry = rowSize - 1;
		String ratioText = row.get(lastEntry).getContent();
		elements[lastEntry] = td().withText(row.get(lastEntry).getContent());
		Matcher matcher = alertPattern.matcher(ratioText);
		if(matcher.find()) elements[lastEntry].withClass("alertRatio");
		
		return elements;
	}
	
	private DataTable writeToFile(String outputDirectory, String fileName, String output) {
		File outFile = new File(outputDirectory + "/" + fileName);
		try (
			BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		) {
			writer.write(output);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//TODO solve unused header information in a better way
		return new FileDataTable(outFile, "", fileName, null);
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
		document.with(style(cssStyles));
		document.with(head().with(meta().withContent("text/html;utf-8")));
		
		return document;
	}
}

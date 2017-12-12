package de.ingef.eva.measures.statistics;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.itextpdf.io.font.PdfEncodings;
import com.itextpdf.io.image.ImageData;
import com.itextpdf.io.image.ImageDataFactory;
import com.itextpdf.kernel.colors.Color;
import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.geom.Rectangle;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfDocumentInfo;
import com.itextpdf.kernel.pdf.PdfPage;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.pdf.canvas.PdfCanvas;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.AreaBreak;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.element.Text;
import com.itextpdf.layout.property.AreaBreakType;
import com.itextpdf.layout.property.HorizontalAlignment;
import com.itextpdf.layout.property.TextAlignment;

import de.ingef.eva.constant.Templates;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class StatisticPdfOutput {

	private static final Color RATIO_HIGHLIGHT = DeviceRgb.RED;
	private static final String OVERVIEW_CAPTION = "Datenstand der Datenbereiche zum elektronischen Datenaustausch der GKV";
	private static final String DETAIL_CAPTION = "Aktueller Datenstand der ambulanten Daten pro KV";
	private static final String MORBI_CAPTIO = "Morbidit\u00e4tsorientierter Risikostrukturausgleich";
	private static final DecimalFormat NUMBER_FORMATTER = (DecimalFormat) NumberFormat.getInstance(Locale.GERMAN);
	private static final double DEVIATION = 0.5;
	private static final String FONT_PATH = "/fonts/Roboto/Roboto-Regular.ttf";
	private static final String LOGO_PATH = "/logos/INGEF_Logo_ohne_claim.jpg";
	private static final float VERTICAL_LOGO_OFFSET = -350f;
	private static final float LOGO_SCALE = 0.05f;
	private static final float CELL_FONT_SIZE = 9f;
	
	public void createStatisticOutput(Path output, List<StatisticsEntry> overviewStatistic, List<StatisticsEntry> detailStatistic, List<MorbiRsaEntry> morbiStatistic, List<String> morbiColumnHeader) {
		try {
			NUMBER_FORMATTER.applyLocalizedPattern("###.###,##");
			PdfFont font = PdfFontFactory.createFont(getClass().getResource(FONT_PATH).toString(), PdfEncodings.CP1250, true);
			Table overview = createOverview(overviewStatistic, font);
			Table details = createDetails(detailStatistic, font);
			Table morbi = createMorbi(morbiStatistic, morbiColumnHeader, font);
			writeToFile(output, overview, details, morbi, font);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Table createMorbi(List<MorbiRsaEntry> morbiStatistic, List<String> columnHeader, PdfFont font) {
		Table table = new Table(5)
				.addHeaderCell(createFilledCell(MORBI_CAPTIO, 1, 5).setFont(font))
				.setHorizontalAlignment(HorizontalAlignment.CENTER);
		columnHeader
			.stream()
			.map(header -> createFilledCell(header).setFont(font))
			.forEachOrdered(cell -> table.addCell(cell));
		morbiStatistic
			.stream()
			.flatMap(e -> Arrays.asList(
					createFilledCell(Integer.toString(e.getGrouperYear())).setFont(font).setFontSize(CELL_FONT_SIZE),
					createFilledCell(Integer.toString(e.getBenefitYear())).setFont(font).setFontSize(CELL_FONT_SIZE),
					createFilledCell(Integer.toString(e.getReportingYear())).setFont(font).setFontSize(CELL_FONT_SIZE),
					createFilledCell(e.getQuarter()).setFont(font).setFontSize(CELL_FONT_SIZE),
					createFilledCell(e.getConfiguration()).setFont(font).setFontSize(CELL_FONT_SIZE)
				).stream()
			)
			.forEachOrdered(cell -> table.addCell(cell));
		return table;
	}
	
	private void writeToFile(Path path, Table overview, Table details,Table morbi, PdfFont font) throws FileNotFoundException {
		PdfDocument output = new PdfDocument(new PdfWriter(path.toString()));
		Document document = new Document(output, PageSize.A4);
		insertMetaData(output);
		document.setTextAlignment(TextAlignment.CENTER);
		insertTitlePage(document, font);
		document.add(new AreaBreak(AreaBreakType.NEXT_PAGE));
		insertRemark(document, font);
		document.setTextAlignment(TextAlignment.CENTER);
		insertVerticalPadding(document, 3, font);
		document.add(overview);
		document.add(new AreaBreak(AreaBreakType.NEXT_PAGE));
		document.add(details);
		document.add(new AreaBreak(AreaBreakType.NEXT_PAGE));
		document.add(morbi);
		document.close();
		
	}

	private void insertRemark(Document document, PdfFont font) {
		document.add(new Paragraph(new Text(Templates.Statistics.HINT_CAPTION).setFont(font)));
		document.setTextAlignment(TextAlignment.LEFT);
		document.add(new Paragraph(new Text(Templates.Statistics.HINT_CONTENT).setFont(font)));
	}

	private void insertTitlePage(Document document, PdfFont font) {
		insertVerticalPadding(document, 25, font);
		Paragraph title = new Paragraph(new Text(encodeDiacritic("EVA-Füllstandsbericht"))
			.setFont(font)
			.setFontSize(24));
		document.add(title);
		Paragraph dateOfCreation = new Paragraph(new Text(LocalDate.now().format(DateTimeFormatter.ofPattern("dd.MM.yyyy")))
			.setFont(font)
			.setFontSize(10));
		document.add(dateOfCreation);
		insertTitleLogo(document);
	}

	private void insertVerticalPadding(Document document, int numberOfEmptyLines, PdfFont font) {
		IntStream
			.range(0, numberOfEmptyLines)
			.forEach(i -> document.add(new Paragraph()));
	}

	private void insertTitleLogo(Document document) {
		try {
			PdfPage page = document.getPdfDocument().getPage(1);
			PdfCanvas canvas = new PdfCanvas(page);
			ImageData logo = ImageDataFactory.create(getClass().getResource(LOGO_PATH).toString());
			float pageWidth = page.getPageSize().getWidth();
			float pageHeight = page.getPageSize().getHeight();
			float logoWidth = logo.getWidth() * LOGO_SCALE;
			float logoHeight = logo.getHeight() * LOGO_SCALE;
			float x = pageWidth / 2f - logoWidth / 2f;
			float y = pageHeight / 2f + logoHeight / 2f + VERTICAL_LOGO_OFFSET;
			canvas.addImage(logo, new Rectangle(x, y, logoWidth, logoHeight), true);
		} catch (MalformedURLException e) {
			log.error("Failed to load report logo. {}", e);
		}
	}
	
	private void insertMetaData(PdfDocument output) {
		PdfDocumentInfo info = output.getDocumentInfo();
		info.setAuthor("InGef Berlin");
		info.setCreator("iText 7");
	}

	private Table createOverview(List<StatisticsEntry> data, PdfFont font) throws IOException {
		Table table = new Table(7)
				.addHeaderCell(createFilledCell(OVERVIEW_CAPTION, 1, 7).setFont(font))
				.setHorizontalAlignment(HorizontalAlignment.CENTER);
		createOverviewHeader(data, font).stream().forEachOrdered(header -> table.addHeaderCell(header));
		Map<Quarter,List<QuarterCount>> transposedData = createQuarterToRowContent(data);
		int rowIndexCounter = 0;
		for(Map.Entry<Quarter, List<QuarterCount>> entry : transposedData.entrySet()) {
			Quarter quarter = entry.getKey();
			Cell rowLabel = createFilledCell(quarter.getYear() + "Q" + quarter.getQuarter())
					.setFont(font)
					.setFontSize(CELL_FONT_SIZE);
			table.addCell(rowLabel);
			int rowIndex = rowIndexCounter++;
			entry.getValue()
				.stream()
				.forEachOrdered(number -> {
					table.addCell(createOverviewContent(number, rowIndex, font));
				});
		}
		return table;
	}

	private Cell createOverviewContent(QuarterCount number, int rowIndex, PdfFont font) {
		StringBuilder sb = new StringBuilder();
		sb.append(NUMBER_FORMATTER.format(number.getCount()));
		Cell cell = new Cell(1,1);
		if(rowIndex > 3) {
			sb.append(" (");
			sb.append(NUMBER_FORMATTER.format(number.getChangeRatio()));
			sb.append(")");
			if(isDeviationTooLarge(number.getChangeRatio()))
				cell.setFontColor(RATIO_HIGHLIGHT);
		}
		cell
			.add(new Paragraph(sb.toString()))
			.setFont(font)
			.setFontSize(CELL_FONT_SIZE);
		return cell;
	}

	private boolean isDeviationTooLarge(double changeRatio) {
		return changeRatio < 1.0 - DEVIATION || changeRatio > 1.0 + DEVIATION;
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
	
	private Table createDetails(List<StatisticsEntry> data, PdfFont font) throws IOException {
		List<List<Cell>> rows = data
			.stream()
			.map(this::convertToRow)
			.collect(Collectors.toList());
		Table table = new Table(5)
				.setHorizontalAlignment(HorizontalAlignment.CENTER)
				.addHeaderCell(
						createFilledCell(DETAIL_CAPTION, 1, 5)
						.setFont(font)
				);
		createDetailsHeader(font).stream().forEachOrdered(header -> table.addHeaderCell(header));
		rows
			.stream()
			.flatMap(row -> row.stream())
			.map(cell -> cell.setFont(font).setFontSize(CELL_FONT_SIZE))
			.forEachOrdered(column -> table.addCell(column));
		return table;
	}
	
	private List<Cell> convertToRow(StatisticsEntry entry) {
		List<Cell> columns = new ArrayList<>(5);	
		QuarterCount qc = entry.getDataCount().get(0);
		Quarter q = qc.getQuarter();
		columns.add(createFilledCell(q.getYear() + "Q" + q.getQuarter()));
		columns.add(createFilledCell(entry.getIdentifier()));
		columns.add(createFilledCell(encodeDiacritic(entry.getLabel())));
		columns.add(createFilledCell(NUMBER_FORMATTER.format(qc.getCount())));
		double ratio = qc.getChangeRatio();
		Cell cell = createFilledCell(NUMBER_FORMATTER.format(ratio));
		if(isDeviationTooLarge(ratio))
			cell.setFontColor(RATIO_HIGHLIGHT);
		columns.add(cell);
		columns.stream().forEach(c -> c.setFontSize(CELL_FONT_SIZE));		
		return columns;
	}
	
	private Cell createFilledCell(String content) {
		return createFilledCell(content, 1, 1);
	}
	
	private Cell createFilledCell(String content, int rowSpan, int colSpan) {
		return new Cell(rowSpan,colSpan).add(new Paragraph(content));
	}
	
	private List<Cell> createDetailsHeader(PdfFont font) {
		return Arrays.asList(
			createFilledCell("Quartal").setFont(font),
			createFilledCell("KV").setFont(font),
			createFilledCell("KV-Name").setFont(font),
			createFilledCell("Fallzahl").setFont(font),
			createFilledCell("Anteil VJQ").setFont(font)
		);
	}
	
	private List<Cell> createOverviewHeader(List<StatisticsEntry> entries, PdfFont font) {
		List<Cell> header = new ArrayList<>(7);
		header.add(createFilledCell("Quartal").setFont(font));
		entries
			.stream()
			.map(entry -> entry.getLabel())
			.forEachOrdered(label -> header.add(createFilledCell(label).setFont(font)));
		return header;
	}
	
	private String encodeDiacritic(String s) {
		return s.replaceAll("ü", "\u00fc");
	}
}

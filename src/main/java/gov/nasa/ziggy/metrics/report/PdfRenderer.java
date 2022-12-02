package gov.nasa.ziggy.metrics.report;

import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.jfree.chart.JFreeChart;

import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Element;
import com.lowagie.text.Font;
import com.lowagie.text.FontFactory;
import com.lowagie.text.Image;
import com.lowagie.text.PageSize;
import com.lowagie.text.Paragraph;
import com.lowagie.text.pdf.PdfPTable;
import com.lowagie.text.pdf.PdfWriter;

/**
 * Thin wrapper around Open PDF and JFreeChart
 *
 * @author Todd Klaus
 */
public class PdfRenderer {
    private Document pdfDocument;
    private final PdfWriter pdfWriter;

    public static final Font titleFont = FontFactory.getFont(FontFactory.HELVETICA, 24, Font.BOLD);
    public static final Font h1Font = FontFactory.getFont(FontFactory.HELVETICA, 14, Font.BOLD);
    public static final Font h2Font = FontFactory.getFont(FontFactory.HELVETICA, 12, Font.BOLD);
    public static final Font bodyFont = FontFactory.getFont(FontFactory.HELVETICA, 10, Font.NORMAL);
    public static final Font bodyBoldFont = FontFactory.getFont(FontFactory.HELVETICA, 10,
        Font.BOLD);
    public static final Font bodyMonoFont = FontFactory.getFont(FontFactory.COURIER, 10,
        Font.NORMAL);

    private final boolean portrait;

    public PdfRenderer(File outputFile) throws Exception {
        this(outputFile, true);
    }

    public PdfRenderer(File outputFile, boolean portrait) throws Exception {
        this.portrait = portrait;

        if (portrait) {
            pdfDocument = new Document(PageSize.LETTER);
        } else {
            pdfDocument = new Document(PageSize.LETTER.rotate());
        }

        FileOutputStream pdfFos = new FileOutputStream(outputFile);
        BufferedOutputStream pdfBos = new BufferedOutputStream(pdfFos);

        pdfWriter = PdfWriter.getInstance(pdfDocument, pdfBos);
        pdfDocument.open();
    }

    public void close() {
        // release resources
        pdfDocument.close();
        pdfDocument = null;
        pdfWriter.close();
    }

    /**
     * Render the chart to the document.
     *
     * @param chart
     * @param width
     * @param height
     * @throws Exception
     */
    public void printChart(JFreeChart chart, int width, int height) throws Exception {
        BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        Image image = Image.getInstance(pdfWriter, bufferedImage, 1.0f);
        pdfDocument.add(image);
    }

    /**
     * Render the chart to the next cell of the specified {@link PdfPTable}
     *
     * @param table
     * @param chart
     * @param width
     * @param height
     * @throws Exception
     */
    public void printChart(PdfPTable table, JFreeChart chart, int width, int height)
        throws Exception {
        BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        Image image = Image.getInstance(pdfWriter, bufferedImage, 1.0f);
        table.addCell(image);
    }

    public void println() throws Exception {
        printText(" ");
    }

    public void printText(String text) throws Exception {
        printText(text, bodyFont);
    }

    public void printText(String text, Font font) throws Exception {
        Paragraph p = new Paragraph(text, font);
        if (font == titleFont) {
            p.setAlignment(Element.ALIGN_CENTER);
        }
        pdfDocument.add(p);
    }

    public void newPage() {
        pdfDocument.newPage();
    }

    public void add(Element element) throws DocumentException {
        pdfDocument.add(element);
    }

    public boolean isPortrait() {
        return portrait;
    }
}

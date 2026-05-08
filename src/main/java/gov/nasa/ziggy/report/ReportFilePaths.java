package gov.nasa.ziggy.report;

import java.nio.file.Path;
import java.util.Date;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * Returns predefined report file names and paths. A suffix of ".pdf" is appended to the filename
 * returned by {@link #performanceReportPath(String, long)}; a suffix of ".txt" is appended by all
 * other methods.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ReportFilePaths {

    public static Path instanceDetailsReportPath(String pipelineName, long instanceId) {
        return reportPath(pipelineName, instanceId, "details", "txt");
    }

    public static Path pipelineReportPath(String pipelineName) {
        return reportPath(pipelineName, -1, "details", "txt");
    }

    public static Path performanceReportPath(String pipelineName, long instanceId) {
        return reportPath(pipelineName, instanceId, "performance", "pdf");
    }

    private static Path reportPath(String pipelineName, long instanceId, String type,
        String extension) {
        String filename = pipelineName.replace(" ", "_") + (instanceId > -1 ? "-" + instanceId : "")
            + "-" + type + "-" + Iso8601Formatter.dateTimeLocalFormatter().format(new Date()) + "."
            + extension;
        return DirectoryProperties.reportsDir().resolve(filename);
    }
}

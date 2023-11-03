package gov.nasa.ziggy.metrics.report;

import java.nio.file.Path;
import java.util.Date;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * Returns predefined report file names and paths, as appropriate.
 *
 * @author PT
 */
public class ReportFilePaths {

    public static Path instanceDetailsReportPath(long instanceId) {

        String filename = "instance-" + instanceId + "-details-"
            + Iso8601Formatter.dateTimeLocalFormatter().format(new Date()) + ".txt";
        return DirectoryProperties.reportsDir().resolve(filename);
    }

    public static Path triggerReportPath(String pipelineName) {
        String filename = "pipeline-" + pipelineName.replace(" ", "_") + "-"
            + Iso8601Formatter.dateTimeLocalFormatter().format(new Date()) + ".txt";
        return DirectoryProperties.reportsDir().resolve(filename);
    }

    public static Path performanceReportPath(long instanceId) {
        String filename = "instance-" + instanceId + "-performance-"
            + Iso8601Formatter.dateTimeLocalFormatter().format(new Date()) + ".pdf";
        return DirectoryProperties.reportsDir().resolve(filename);
    }
}

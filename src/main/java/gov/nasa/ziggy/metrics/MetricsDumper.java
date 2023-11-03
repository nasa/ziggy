package gov.nasa.ziggy.metrics;

import static gov.nasa.ziggy.metrics.MetricsDumper.FileReuseMode.ReuseFile;
import static gov.nasa.ziggy.metrics.MetricsDumper.FileReuseMode.RotateFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.output.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * {@link Runnable} that periodically dumps metrics to a file.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 */
public class MetricsDumper implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MetricsDumper.class);

    enum FileReuseMode {
        ReuseFile, RotateFile;
    }

    private static int DUMP_INTERVAL_MILLIS = 30000;
    private static final int MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024 * 2;
    private static final int BUF_SIZE_BYTES = 1024 * 1024;

    private PrintWriter printWriter;
    private CountingOutputStream countOut;
    private final File metricsFile;

    public MetricsDumper(long pid) {
        this(pid, DirectoryProperties.supervisorLogDir());
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public MetricsDumper(long pid, Path metricDumpDir) {
        try {
            Files.createDirectories(metricDumpDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create directory " + metricDumpDir.toString(),
                e);
        }
        metricsFile = metricDumpDir.resolve("metrics-dump-" + pid + ".txt").toFile();
        openFile(RotateFile);
    }

    /**
     * Close old stuff, rotate and create a new log file.
     *
     * @param metricsFile
     * @throws IOException
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void openFile(FileReuseMode reuseMode) {
        if (countOut != null && countOut.getCount() < MAX_FILE_SIZE_BYTES) {
            return;
        }
        if (printWriter != null) {
            printWriter.close();
        }
        try {
            if (metricsFile.exists() && (reuseMode == RotateFile
                || reuseMode == ReuseFile && metricsFile.length() >= MAX_FILE_SIZE_BYTES)) {
                File oldFile = new File(metricsFile.getParent(), metricsFile.getName() + ".old");
                if (oldFile.exists() && !oldFile.delete()) {
                    throw new IOException("Failed to delete file \"" + oldFile + "\".");
                }
                metricsFile.renameTo(oldFile);
            }
            FileOutputStream fout = new FileOutputStream(metricsFile, true /* append mode */);
            BufferedOutputStream bout = new BufferedOutputStream(fout, BUF_SIZE_BYTES);
            countOut = new CountingOutputStream(bout);
            printWriter = new PrintWriter(new OutputStreamWriter(countOut, FileUtil.ZIGGY_CHARSET));
        } catch (IOException e) {
            throw new UncheckedIOException("IOException occurred on file " + metricsFile.toString(),
                e);
        }
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_IN_RUNNABLE)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void run() {
        try {
            while (true) {
                Thread.sleep(DUMP_INTERVAL_MILLIS);
                Metric.dump(printWriter);
                printWriter.flush();
                openFile(RotateFile);
            }
        } catch (InterruptedException ie) {
            log.debug("Metrics dumper exiting on interrupted exception.");
        } catch (Exception e) {
            log.error("Failed to dump metrics.", e);
        }
    }
}

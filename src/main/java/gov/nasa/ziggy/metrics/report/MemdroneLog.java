package gov.nasa.ziggy.metrics.report;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * @author Todd Klaus
 */
public class MemdroneLog {
    private static final Logger log = LoggerFactory.getLogger(MemdroneLog.class);

    private InputStream input;
    private int lineCount = 0;
    private int skipCount = 0;
    private File memdroneLogFile;
    // Map<ProcessId, DescriptiveStats>
    private Map<String, DescriptiveStatistics> logContents;

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public MemdroneLog(File memdroneLogFile) {
        if (!memdroneLogFile.exists()) {
            throw new PipelineException(
                "Specified memdrone file " + memdroneLogFile + " does not exist");
        }

        if (!memdroneLogFile.isFile()) {
            throw new PipelineException(
                "Specified memdrone file " + memdroneLogFile + " is not a regular file");
        }

        try {
            input = new FileInputStream(memdroneLogFile);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException("File " + memdroneLogFile + " not found", e);
        }
        this.memdroneLogFile = memdroneLogFile;
        parse();
    }

    public MemdroneLog(InputStream input) {
        this.input = input;
        parse();
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void parse() {
        log.info("Parsing");

        logContents = new HashMap<>();

        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(input, ZiggyFileUtils.ZIGGY_CHARSET))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                lineCount++;
                MemdroneSample s = new MemdroneSample(line);
                if (s.isValid()) {
                    DescriptiveStatistics stats = logContents.get(s.getProcessId());
                    if (stats == null) {
                        stats = new DescriptiveStatistics();
                        logContents.put(s.getProcessId(), stats);
                    }
                    stats.addValue(s.getMemoryKilobytes() * 1024.0);
                } else {
                    skipCount++;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "IOException occurred reading from " + memdroneLogFile.toString(), e);
        }

        log.info("Parsing...done");
        log.info("lineCount = {}", lineCount);
        log.info("skipCount = {}", skipCount);
    }

    public Map<String, DescriptiveStatistics> getLogContents() {
        return logContents;
    }
}

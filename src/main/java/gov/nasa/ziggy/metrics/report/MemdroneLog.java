package gov.nasa.ziggy.metrics.report;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * @author Todd Klaus
 */
public class MemdroneLog {
    private static final Logger log = LoggerFactory.getLogger(MemdroneLog.class);

    private InputStream input;
    private int lineCount = 0;
    private int skipCount = 0;
    // Map<ProcessId, DescriptiveStats>
    private Map<String, DescriptiveStatistics> logContents;

    public MemdroneLog(File memdroneLogFile) {
        if (!memdroneLogFile.exists()) {
            throw new PipelineException(
                "Specified memdrone file does not exist: " + memdroneLogFile);
        }

        if (!memdroneLogFile.isFile()) {
            throw new PipelineException(
                "Specified memdrone file is not a regular file: " + memdroneLogFile);
        }

        try {
            input = new FileInputStream(memdroneLogFile);
            parse();
        } catch (Exception e) {
            throw new PipelineException("failed to parse file, caught e = " + e, e);
        }
    }

    public MemdroneLog(InputStream input) {
        this.input = input;
        try {
            parse();
        } catch (Exception e) {
            throw new PipelineException("failed to parse file, caught e = " + e, e);
        }
    }

    private void parse() throws Exception {
        log.info("Parse started");

        logContents = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(input))) {
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
        }

        log.info("Parse complete");
        log.info("lineCount: " + lineCount);
        log.info("skipCount: " + skipCount);
    }

    public Map<String, DescriptiveStatistics> getLogContents() {
        return logContents;
    }
}

package gov.nasa.ziggy.metrics.report;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * A single line from a memdrone log file. Lines in the memdrone log take the following form:
 *
 * <pre>
 * LINE:   Tue Aug 14 06:39:13 PDT 2012 43085 845252 tps
 * FIELD#: 0   1   2  3        4   5    6     7      8
 * </pre>
 *
 * @author Todd Klaus
 */
public class MemdroneSample {

    private static final Logger log = LoggerFactory.getLogger(MemdroneSample.class);

    private String processName = "";
    private String processId = "";
    private long timestampMillis = 0L;
    private int memoryKilobytes = 0;

    private boolean valid = false;

    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(
        "EEE MMM dd kk:mm:ss zzz yyyy");

    public MemdroneSample(String memdroneLogLine) {
        valid = parse(memdroneLogLine);
    }

    public MemdroneSample(String processName, String processId, long timestampMillis,
        float percentCpu, float percentMemory, int memoryKilobytes) {
        this.processName = processName;
        this.processId = processId;
        this.timestampMillis = timestampMillis;
        this.memoryKilobytes = memoryKilobytes;

        valid = true;
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private boolean parse(String memdroneLogLine) {
        String[] elements = memdroneLogLine.split("\\s+");

        if (elements.length != 9) {
            log.warn("Parse error, {} elements != 9", memdroneLogLine);
            return false;
        }
        String timestampString = elements[0] + " " + // day of week
            elements[1] + " " + // month
            elements[2] + " " + // date
            elements[3] + " " + // time
            elements[4] + " " + // TZ
            elements[5]; // year
        processId = elements[6];
        processName = elements[8];

        try {
            timestampMillis = parseDate(timestampString);
            memoryKilobytes = Integer.parseInt(elements[7]);
        } catch (ParseException | NumberFormatException e) {
            log.warn("Parse error {}", e);
            return false;
        }
        return true;
    }

    private long parseDate(String s) throws ParseException {
        return timestampFormat.parse(s).getTime();
    }

    public String getProcessName() {
        return processName;
    }

    public String getProcessId() {
        return processId;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public int getMemoryKilobytes() {
        return memoryKilobytes;
    }

    public boolean isValid() {
        return valid;
    }

    @Override
    public String toString() {
        return "MemdroneSample [processName=" + processName + ", processId=" + processId
            + ", memoryKilobytes=" + memoryKilobytes + ", timestampMillis=" + timestampMillis
            + ", valid=" + valid + "]";
    }
}

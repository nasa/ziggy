package gov.nasa.ziggy.module.remote;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Extracts the exit comment and exit status from one or more PBS logs.
 *
 * @author PT
 */
public class PbsLogParser {

    private static final Logger log = LoggerFactory.getLogger(PbsLogParser.class);

    public static final String PBS_FILE_COMMENT_PREFIX = "=>> PBS: ";
    public static final String PBS_FILE_STATUS_PREFIX = "Exit Status";

    /**
     * Extracts the exit comments from a collection of PBS logs and returns in a {@link Map} with
     * job ID as the map key. Jobs with no comment will have no entry in the map.
     */
    public Map<Long, String> exitCommentByJobId(
        Collection<RemoteJobInformation> remoteJobsInformation) {
        Map<Long, String> exitCommentByJobId = new HashMap<>();
        if (CollectionUtils.isEmpty(remoteJobsInformation)) {
            return exitCommentByJobId;
        }
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            String exitComment = exitComment(remoteJobInformation);
            if (!StringUtils.isBlank(exitComment)) {
                exitCommentByJobId.put(remoteJobInformation.getJobId(), exitComment);
            }
        }
        return exitCommentByJobId;
    }

    /** Returns the exit comment from a PBS log, or null if there is no exit comment. */
    private String exitComment(RemoteJobInformation remoteJobInformation) {
        List<String> pbsFileOutput = pbsLogFileContent(remoteJobInformation);
        if (CollectionUtils.isEmpty(pbsFileOutput)) {
            return null;
        }
        for (String pbsFileOutputLine : pbsFileOutput) {
            log.debug("PBS file output line: {}", pbsFileOutputLine);
            if (pbsFileOutputLine.startsWith(PBS_FILE_COMMENT_PREFIX)) {
                return pbsFileOutputLine.substring(PBS_FILE_COMMENT_PREFIX.length());
            }
        }
        return null;
    }

    /** Returns the content of a PBS log file as a {@List} of {@link String}s. */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private List<String> pbsLogFileContent(RemoteJobInformation remoteJobInformation) {
        try {
            return Files.readAllLines(Paths.get(remoteJobInformation.getLogFile()));
        } catch (IOException e) {
            // If an exception occurred, we don't want to crash the algorithm monitor,
            // so return null.
            return null;
        }
    }

    /**
     * Extracts the exit status from a collection of PBS log files and returns as a {@link Map} with
     * job ID as the map key. Jobs with no exit status will have no entry in the map.
     */
    public Map<Long, Integer> exitStatusByJobId(
        Collection<RemoteJobInformation> remoteJobsInformation) {
        Map<Long, Integer> exitStatusByJobId = new HashMap<>();
        if (CollectionUtils.isEmpty(remoteJobsInformation)) {
            return exitStatusByJobId;
        }
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            Integer exitStatus = exitStatus(remoteJobInformation);
            if (exitStatus != null) {
                exitStatusByJobId.put(remoteJobInformation.getJobId(), exitStatus);
            }
        }
        return exitStatusByJobId;
    }

    /** Returns the exit status from a PBS log file, or null if there is no exit status. */
    private Integer exitStatus(RemoteJobInformation remoteJobInformation) {
        List<String> pbsFileOutput = pbsLogFileContent(remoteJobInformation);
        if (CollectionUtils.isEmpty(pbsFileOutput)) {
            return null;
        }
        for (String pbsFileOutputLine : pbsFileOutput) {
            log.debug("PBS file output line: {}", pbsFileOutputLine);
            if (pbsFileOutputLine.trim().startsWith(PBS_FILE_STATUS_PREFIX)) {
                int colonLocation = pbsFileOutputLine.indexOf(":");
                String returnStatusString = pbsFileOutputLine.substring(colonLocation + 1).trim();
                return Integer.parseInt(returnStatusString);
            }
        }
        return null;
    }
}

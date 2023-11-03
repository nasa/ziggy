package gov.nasa.ziggy.module.remote;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * A {@link QueueCommandManager} for issuing queue commands locally.
 *
 * @author PT
 */
public class QueueLocalCommandManager extends QueueCommandManager {

    private static final Logger log = LoggerFactory.getLogger(QueueLocalCommandManager.class);

    private static final String QSTAT = "qstat ";
    private static final String QDEL = "qdel ";

    @Override
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    protected List<String> qstat(String commandArgs, String... targets) {

        List<String> qstatResults = new ArrayList<>();
        ExternalProcess p = ExternalProcess.simpleExternalProcess(QSTAT + commandArgs);
        p.logStdErr(false);
        p.logStdOut(false);
        p.writeStdErr(true);
        p.writeStdOut(true);
        try {
            p.run(true, 0);
            qstatResults = p.stdout(targets);
        } catch (Exception e) {
            // The qstat program is not under our control and can fail due to
            // various transient file system and network issues. If this happens,
            // and results a runtime exception, we don't want it to bring down
            // the monitoring system, so we catch all possible exceptions here
            // and keep going, in hopes that the next time the user calls qstat
            // the transient problem has resolved itself.
            log.error("Error when attempting to run qstat command", e);
        }
        return qstatResults;
    }

    @Override
    protected void qdel(String qdelArgs) {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(QDEL + qdelArgs);
        log.info("Executing command: " + qdelArgs);
        p.run(false, 0);
    }
}

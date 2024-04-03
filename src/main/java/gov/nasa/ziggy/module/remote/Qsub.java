package gov.nasa.ziggy.module.remote;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * Calls qsub, the Portable Batch System (PBS) command to submit a batch job.
 *
 * <pre>
 * SCRIPT_ARGS="$WORKING_DIR $ZIGGY_HOME $STATE_FILE_PATH" QSUB_ARGS= "-N $JOB_NAME -q
 * $QUEUE_NAME -rn -l walltime=$WALL_TIME -l select=$NUM_NODES:model=$ARCH_TYPE -W
 * group_list=$GROUP_NAME"
 *
 * echo ${ZIGGY_BIN_DIR}/nas-task-master.sh ${SCRIPT_ARGS} | qsub ${QSUB_ARGS} or qsub ${QSUB_ARGS}
 * -- nas-task-master.sh ${SCRIPT_ARGS}
 * </pre>
 *
 * @author Todd Klaus
 */
public class Qsub {
    private static final Logger log = LoggerFactory.getLogger(Qsub.class);

    private static final int QSUB_TIMEOUT_SECS = 60;

    // Note: the "primitive" members are boxed so that the validate method can tell whether they
    // have been set by looking for null values.

    private String queueName;
    private String wallTime;
    private Integer numNodes;
    private Integer coresPerNode;
    private Double gigsPerNode;
    private String model;
    private String groupName;
    private String datestamp = Iso8601Formatter.dateTimeLocalFormatter().format(new Date());

    private String scriptPath;
    private String taskDir;
    private String ziggyProgram;
    private String pbsLogDir;
    private String nasLogDir;
    private SupportedRemoteClusters cluster;

    private PipelineTask pipelineTask;

    private Qsub() {
    }

    public int[] submitMultipleJobsForTask() {
        int[] returnCodes = new int[numNodes];
        for (int iJob = 0; iJob < numNodes; iJob++) {
            returnCodes[iJob] = submit1Job(iJob);
        }
        return returnCodes;
    }

    private int submit1Job(int jobIndex) {

        CommandLine commandLine = new CommandLine("/PBS/bin/qsub");
        commandLine.addArgument("-N");
        commandLine.addArgument(fullJobName(jobIndex));
        commandLine.addArgument("-q");
        commandLine.addArgument(queueName);
        commandLine.addArgument("-rn");
        commandLine.addArgument("-l");
        commandLine.addArgument(resourceOptions(jobIndex < 0));
        commandLine.addArgument("-W");
        commandLine.addArgument("group_list=" + groupName);
        String environment = environment();
        if (!environment.isEmpty()) {
            commandLine.addArgument("-v");
            commandLine.addArgument(environment);
        }
        commandLine.addArgument("-o");
        commandLine.addArgument(pbsLogFile(jobIndex));
        commandLine.addArgument("-j");
        commandLine.addArgument("oe");
        commandLine.addArgument("--");
        commandLine.addArgument(scriptPath);
        commandLine.addArgument(ziggyProgram);

        commandLine.addArgument(taskDir);
        commandLine.addArgument(algorithmLogFile(jobIndex));

        log.info("commandLine: " + commandLine);
        for (String arg : commandLine.getArguments()) {
            log.info("arg: " + arg);
        }

        log.info("Running PBS command: " + commandLine);
        return runCommandLine(commandLine, QSUB_TIMEOUT_SECS);
    }

    private String fullJobName(int jobIndex) {
        return pipelineTask.taskBaseName() + "." + jobIndex;
    }

    /**
     * Concatenates the internal ziggy.environment and user-defined ziggy.pipeline.environment
     * variables for use by the qsub -v option.
     */
    private String environment() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        String environment = config.getString(PropertyName.ZIGGY_RUNTIME_ENVIRONMENT.property(),
            "");
        String userEnvironment = config.getString(PropertyName.RUNTIME_ENVIRONMENT.property(), "");
        if (!userEnvironment.isEmpty()) {
            environment = environment + (!environment.isEmpty() ? "," : "") + userEnvironment;
        }
        return environment;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private String pbsLogFile(int jobIndex) {
        File pbsLogFile = new File(pbsLogDir, "pbs-" + fullJobName(jobIndex) + "-" + datestamp);
        try {
            return pbsLogFile.getCanonicalPath();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to get path to file " + pbsLogFile.toString(),
                e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private String algorithmLogFile(int jobIndex) {
        File nasLogFile = new File(nasLogDir, pipelineTask.logFilename(jobIndex));
        try {
            return nasLogFile.getCanonicalPath();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to get path to file " + nasLogFile.toString(),
                e);
        }
    }

    private String resourceOptions(boolean multiNodeJob) {
        int numNodesInCall = 1;
        if (multiNodeJob) {
            numNodesInCall = numNodes;
        }
        String options = null;
        if (cluster.equals(SupportedRemoteClusters.AWS)) {
            options = "walltime=" + wallTime + ",select=" + numNodesInCall + ":arch=" + model
                + ":ncpus=" + coresPerNode + ":mem=" + gigsPerNode + "g";
        } else {
            options = "walltime=" + wallTime + ",select=" + numNodesInCall + ":model=" + model;
        }
        return options;
    }

    private int runCommandLine(CommandLine commandline, int timeoutSecs) {
        ExternalProcess p = ExternalProcess.stdoutLoggingExternalProcess(commandline);

        // log to stdout/stderr & Log4j.
        p.logStdErr(true);

        return p.run(true, timeoutSecs * 1000);
    }

    private void validate() {
        List<String> unsetParameters = new ArrayList<>();
        if (queueName == null) {
            unsetParameters.add("queueName");
        }
        if (wallTime == null) {
            unsetParameters.add("wallTime");
        }
        if (numNodes == null) {
            unsetParameters.add("numNodes");
        }
        if (cluster == null) {
            unsetParameters.add("cluster");
        } else if (cluster.equals(SupportedRemoteClusters.AWS)) {
            if (coresPerNode == null) {
                unsetParameters.add("coresPerNode");
            }
            if (gigsPerNode == null) {
                unsetParameters.add("gigsPerNode");
            }
        }
        if (model == null) {
            unsetParameters.add("model");
        }
        if (groupName == null) {
            unsetParameters.add("groupName");
        }
        if (scriptPath == null) {
            unsetParameters.add("scriptPath");
        }
        if (ziggyProgram == null) {
            unsetParameters.add("ziggyProgram");
        }
        if (taskDir == null) {
            unsetParameters.add("taskDir");
        }
        if (pbsLogDir == null) {
            unsetParameters.add("pbsLogDir");
        }
        if (nasLogDir == null) {
            unsetParameters.add("nasLogDir");
        }
        if (pipelineTask == null) {
            unsetParameters.add("pipelineTask");
        }
        if (!unsetParameters.isEmpty()) {
            log.error("Qsub missing required parameters: ["
                + StringUtils.join(unsetParameters.iterator(), ",") + "]");
            throw new IllegalStateException("Qsub missing required parameters");
        }
    }

    // Setters: all private so only accessible to the Builder, below.
    private void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    private void setWallTime(String wallTime) {
        this.wallTime = wallTime;
    }

    private void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }

    private void setCoresPerNode(int coresPerNode) {
        this.coresPerNode = coresPerNode;
    }

    private void setGigsPerNode(double gigsPerNode) {
        this.gigsPerNode = gigsPerNode;
    }

    private void setModel(String model) {
        this.model = model;
    }

    private void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    private void setScriptPath(String scriptPath) {
        this.scriptPath = scriptPath;
    }

    private void setCluster(SupportedRemoteClusters cluster) {
        this.cluster = cluster;
    }

    private void setPbsLogDir(String logDir) {
        pbsLogDir = logDir;
    }

    private void setNasLogDir(String logDir) {
        nasLogDir = logDir;
    }

    private void setPipelineTask(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    private void setZiggyProgram(String ziggyProgram) {
        this.ziggyProgram = ziggyProgram;
    }

    private void setTaskDir(String taskDir) {
        this.taskDir = taskDir;
    }

    /**
     * Builder for {@link Qsub} instances. Ensures that all necessary arguments are provided and
     * that internal consistency is preserved.
     *
     * @author PT
     */
    public static class Builder {

        private Qsub qsub;

        public Builder() {
            qsub = new Qsub();
        }

        public Builder queueName(String queueName) {
            qsub.setQueueName(queueName);
            return this;
        }

        public Builder wallTime(String wallTime) {
            qsub.setWallTime(wallTime);
            return this;
        }

        public Builder numNodes(int numNodes) {
            qsub.setNumNodes(numNodes);
            return this;
        }

        public Builder coresPerNode(int coresPerNode) {
            qsub.setCoresPerNode(coresPerNode);
            return this;
        }

        public Builder gigsPerNode(double gigsPerNode) {
            qsub.setGigsPerNode(gigsPerNode);
            return this;
        }

        public Builder model(String model) {
            qsub.setModel(model);
            return this;
        }

        public Builder groupName(String groupName) {
            qsub.setGroupName(groupName);
            return this;
        }

        public Builder scriptPath(String scriptPath) {
            qsub.setScriptPath(scriptPath);
            return this;
        }

        public Builder ziggyProgram(String ziggyProgram) {
            qsub.setZiggyProgram(ziggyProgram);
            return this;
        }

        public Builder cluster(SupportedRemoteClusters cluster) {
            qsub.setCluster(cluster);
            return this;
        }

        public Builder pbsLogDir(String logDir) {
            qsub.setPbsLogDir(logDir);
            return this;
        }

        public Builder nasLogDir(String nasLogDir) {
            qsub.setNasLogDir(nasLogDir);
            return this;
        }

        public Builder pipelineTask(PipelineTask pipelineTask) {
            qsub.setPipelineTask(pipelineTask);
            return this;
        }

        public Builder taskDir(String taskDir) {
            qsub.setTaskDir(taskDir);
            return this;
        }

        public Qsub build() {

            qsub.validate();
            return qsub;
        }
    }
}

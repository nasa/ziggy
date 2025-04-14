package gov.nasa.ziggy.pipeline.step.remote.batch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.AlgorithmExecutor;
import gov.nasa.ziggy.pipeline.step.remote.BatchManager;
import gov.nasa.ziggy.pipeline.step.remote.RemoteJobInformation;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.logging.ZiggyLog;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.HostNameUtils;
import gov.nasa.ziggy.util.Iso8601Formatter;
import gov.nasa.ziggy.util.TimeFormatter;

public class PbsBatchManager implements BatchManager<PbsBatchParameters> {

    // Parameters related to the qsub command.
    private static final String JOB_NAME_OPTION = "-N";
    private static final String QUEUE_NAME_OPTION = "-q";
    private static final String RESTART_OPTION = "-rn";
    private static final String RESOURCE_OPTION = "-l";
    private static final String ATTRIBUTES_OPTION = "-W";
    private static final String GROUP_LIST_ARGUMENT = "group_list=";
    private static final String ENVIRONMENT_OPTION = "-v";
    private static final String OUTPUT_OPTION = "-o";
    private static final String JOIN_OPTION = "-j";
    private static final String JOIN_ARG = "oe";
    private static final String END_QSUB_ARGS_INDICATOR = "--";
    private static final String PBS_LOGFILE_PREFIX = "pbs-";
    private static final String QSUB = "qsub";

    // Parameters related to the qstat command.
    private static final String QSTAT = "qstat ";

    // Indices for the various components of a qstat output line
    public static final int ID_INDEX = 0;
    public static final int OWNER_INDEX = 1;
    public static final int QUEUE_INDEX = 2;
    public static final int NAME_INDEX = 3;
    public static final int TSK_INDEX = 4;
    public static final int NODES_INDEX = 5;
    public static final int REQUESTED_TIME_INDEX = 6;
    public static final int STATUS_INDEX = 7;
    public static final int ELAPSED_TIME_INDEX = 8;
    public static final int EFF_INDEX = 9;

    public static final String SELECT = "Resource_List.select";
    public static final String WALLTIME = "resources_used.walltime";
    public static final String JOBNAME = "Job_Name";
    public static final String OUTPUT_PATH = "Output_Path";
    public static final Pattern SELECT_PATTERN = Pattern
        .compile("\\s*" + SELECT + " = ([0-9]+):model=(\\S+)");
    public static final Pattern WALLTIME_PATTERN = Pattern.compile("\\s*" + WALLTIME + " = (\\S+)");
    public static final Pattern JOBNAME_PATTERN = Pattern.compile("\\s*" + JOBNAME + " = (\\S+)");
    public static final Pattern OUTPUT_PATH_PATTERN = Pattern
        .compile("\\s*" + OUTPUT_PATH + " = [^:]+:(\\S+)");
    public static final String QSTAT_JOB_ROW_NAME = "Job:";
    public static final String QSTAT_OWNER_ROW_NAME = "Job_Owner";
    public static final String QSTAT_EXIT_STATUS_ROW_NAME = "Exit_status";

    // Parameters related to the PBS log file.
    public static final String PBS_FILE_COMMENT_PREFIX = "=>> PBS: ";
    public static final String PBS_FILE_STATUS_PREFIX = "Exit Status";

    // Parameters related to the qdel command.
    private static final String QDEL = "qdel ";

    private static final Logger log = LoggerFactory.getLogger(PbsBatchManager.class);

    private static final int QSUB_TIMEOUT_SECS = 60;

    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTask pipelineTask;
    private PipelineNodeExecutionResources executionResources;
    private PbsBatchParameters pbsBatchParameters;
    private String datestamp = Iso8601Formatter.dateTimeLocalFormatter().format(new Date());
    private List<CommandLine> commandLines = new ArrayList<>();
    private String user;
    private String host;

    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public List<RemoteJobInformation> submitJobs(PipelineTask pipelineTask, int subtaskCount) {

        setPipelineTask(pipelineTask);
        List<RemoteJobInformation> remoteJobsInformation = new ArrayList<>();
        pbsBatchParameters = new PbsBatchParameters();
        pbsBatchParameters.computeParameterValues(executionResources, subtaskCount);
        createPbsLogDirectory();

        // Execute the PBS commands in a thread pool executor so that we don't have to wait
        // for one submission to return before starting the next.
        ThreadPoolExecutor jobSubmissionExecutor = (ThreadPoolExecutor) Executors
            .newFixedThreadPool(pbsBatchParameters.nodeCount());
        List<Future<RemoteJobInformation>> informationFutures = new ArrayList<>();
        for (int jobIndex = 0; jobIndex < pbsBatchParameters.nodeCount(); jobIndex++) {
            CallableJobSubmitter jobSubmitter = new CallableJobSubmitter(jobIndex);
            informationFutures.add(jobSubmissionExecutor.submit(jobSubmitter));
        }

        // If the thread failed for any reason, return a RemoteJobInformation instance
        // that has an exit code that indicates failure.
        for (Future<RemoteJobInformation> informationFuture : informationFutures) {
            try {
                remoteJobsInformation.add(informationFuture.get());
            } catch (InterruptedException e) {
                log.error("Interrupted submission thread", e);
                RemoteJobInformation failedJob = new RemoteJobInformation("", "", "");
                failedJob.setBatchSubmissionExitCode(1);
                remoteJobsInformation.add(failedJob);
            } catch (ExecutionException e) {
                log.error("Execution exception in submission thread", e);
                RemoteJobInformation failedJob = new RemoteJobInformation("", "", "");
                failedJob.setBatchSubmissionExitCode(2);
                remoteJobsInformation.add(failedJob);
            }
        }
        return remoteJobsInformation;
    }

    /** Submits a single job to PBS via an {@link ExternalProcess} instance. */
    private RemoteJobInformation submitOneJob(int jobIndex) {

        // Construct the command line.
        CommandLine commandLine = commandLine(jobIndex);
        commandLines.add(commandLine);
        log.info("Running PBS command: {}", commandLine);
        RemoteJobInformation remoteJobInformation = new RemoteJobInformation(pbsLogFile(jobIndex),
            fullJobName(jobIndex), executionResources.getRemoteEnvironment().getName());

        int exitCode = runQsubCommandLine(commandLine, QSUB_TIMEOUT_SECS);
        remoteJobInformation.setBatchSubmissionExitCode(exitCode);
        remoteJobInformation.setCostFactor(pbsBatchParameters.getArchitecture().getCost());
        return remoteJobInformation;
    }

    /** Constructs a CommandLine instance for a single job. */
    private CommandLine commandLine(int jobIndex) {
        CommandLine commandLine = new CommandLine(QSUB);
        commandLine.addArgument(JOB_NAME_OPTION);
        commandLine.addArgument(fullJobName(jobIndex));
        commandLine.addArgument(QUEUE_NAME_OPTION);
        commandLine.addArgument(pbsBatchParameters.getBatchQueue().getName());
        commandLine.addArgument(RESTART_OPTION);
        commandLine.addArgument(RESOURCE_OPTION);
        commandLine.addArgument(jobResources(pbsBatchParameters));
        commandLine.addArgument(ATTRIBUTES_OPTION);
        commandLine.addArgument(GROUP_LIST_ARGUMENT + pbsBatchParameters.getRemoteGroup());
        String environment = environment();
        if (!StringUtils.isBlank(environment)) {
            commandLine.addArgument(ENVIRONMENT_OPTION);
            commandLine.addArgument(environment);
        }
        commandLine.addArgument(OUTPUT_OPTION);
        commandLine.addArgument(pbsLogFile(jobIndex));
        commandLine.addArgument(JOIN_OPTION);
        commandLine.addArgument(JOIN_ARG);
        commandLine.addArgument(END_QSUB_ARGS_INDICATOR);
        commandLine.addArgument(scriptPath());
        commandLine.addArgument(algorithmLogFileSystemProperty(jobIndex));
        commandLine.addArgument(ZiggyLog.singleFileAppenderSystemProperty());
        commandLine.addArgument(ZiggyLog.algorithmNameSystemProperty(pipelineTask));
        commandLine.addArgument(AlgorithmExecutor.NODE_MASTER_NAME);
        commandLine.addArgument(DirectoryProperties.taskDataDir()
            .resolve(pipelineTask.taskBaseName())
            .toAbsolutePath()
            .toString());
        return commandLine;
    }

    /** The full job name, which is the task name plus "." plus a job index. */
    private String fullJobName(int jobIndex) {
        return pipelineTask.taskBaseName() + "." + jobIndex;
    }

    /** The job resources argument, which includes wall time and architecture. */
    private String jobResources(PbsBatchParameters pbsBatchParameters) {
        String resourceFormat = "walltime=%s,select=1:model=%s";
        return String.format(resourceFormat, pbsBatchParameters.getRequestedWallTime(),
            pbsBatchParameters.getArchitecture().getName());
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
        if (!userEnvironment.isBlank()) {
            environment = environment + (!environment.isBlank() ? "," : "") + userEnvironment;
        }
        return environment;
    }

    /** The full name and path for the PBS log file. */
    private String pbsLogFile(int jobIndex) {
        Path pbsLogFile = pbsLogDir().resolve(pbsLogFileName(jobIndex));
        return pbsLogFile.toAbsolutePath().toString();
    }

    private Path pbsLogDir() {
        return DirectoryProperties.logDir().resolve(SupportedBatchSystem.PBS.logFileRelativePath());
    }

    /** The file name for the PBS log file. */
    private String pbsLogFileName(int jobIndex) {
        return PBS_LOGFILE_PREFIX + fullJobName(jobIndex) + "-" + datestamp;
    }

    /** The full path to the Ziggy perl script. */
    private String scriptPath() {
        return DirectoryProperties.ziggyBinDir()
            .resolve(AlgorithmExecutor.ZIGGY_PROGRAM)
            .toAbsolutePath()
            .toString();
    }

    /** Executes the command line for the qsub call. */
    private int runQsubCommandLine(CommandLine commandLine, int timeoutSecs) {
        return qsubExternalProcess(commandLine).run(true, timeoutSecs * 1000);
    }

    /** Returns a {@link Map} of the job ID for each job for a given {@link PipelineTask}. */
    @Override
    public Map<String, Long> jobIdByName(PipelineTask pipelineTask) {

        setPipelineTask(pipelineTask);
        Map<String, Long> jobIdByName = new HashMap<>();

        // Use qstat to obtain the jobs that match the pipeline task.
        List<String> jobsInfoFromQstat = qstat("-u " + user(), pipelineTask.taskBaseName());
        if (jobsInfoFromQstat.isEmpty()
            || jobsInfoFromQstat.size() == 1 && jobsInfoFromQstat.get(0).isBlank()) {
            return jobIdByName;
        }

        // Construct a map of the jobs by ID first.
        Map<Long, String> nameByJobId = new HashMap<>();
        for (String jobInfoFromQstat : jobsInfoFromQstat) {
            nameByJobId.put(jobIdFromQstat(jobInfoFromQstat), jobNameFromQstat(jobInfoFromQstat));
        }

        // Eliminate any jobs that were submitted from a different server.
        Set<Long> jobIdsThisServer = jobIdsThisServer(nameByJobId.keySet());
        if (jobIdsThisServer.isEmpty()) {
            return jobIdByName;
        }
        for (long jobId : jobIdsThisServer) {
            jobIdByName.put(nameByJobId.get(jobId), jobId);
        }
        return jobIdByName;
    }

    /** Returns the subset of job IDs that were submitted by the current server. */
    private Set<Long> jobIdsThisServer(Set<Long> allJobIds) {
        Set<Long> jobIdsThisServer = new TreeSet<>(); // Deterministic order.
        List<String> qstatOutput = qstat("-xf " + StringUtils.join(allJobIds, " "),
            QSTAT_JOB_ROW_NAME, QSTAT_OWNER_ROW_NAME);

        // parse the output: there are 2 lines per job, the first has the ID and the
        // second has the server name, though both need to be chopped up
        for (int qstatOutputIndex = 0; qstatOutputIndex < qstatOutput
            .size(); qstatOutputIndex += 2) {
            Long jobId = Long.valueOf(StringUtils.stripStart(
                qstatOutput.get(qstatOutputIndex).split("\\.")[0], QSTAT_JOB_ROW_NAME + " "));
            String jobServerName = qstatOutput.get(qstatOutputIndex + 1).split("\\@")[1]
                .split("\\.")[0];
            if (jobServerName.equals(host())) {
                jobIdsThisServer.add(jobId);
            }
        }
        return jobIdsThisServer;
    }

    /** Parses the qstat -xf output to obtain the job ID. */
    private long jobIdFromQstat(String qstatLine) {
        String[] qstatLineParts = qstatLine.split("\\s+");
        String jobId = qstatLineParts[ID_INDEX];
        String[] jobIdParts = jobId.split("\\.");
        return Long.parseLong(jobIdParts[0]);
    }

    /** Parses the qstat -xf output to obtain the job name. */
    private String jobNameFromQstat(String qstatLine) {
        String[] qstatLineParts = qstatLine.split("\\s+");
        return qstatLineParts[NAME_INDEX];
    }

    /** Indicates whether a given job is finished, indicated by the presence of its log file. */
    @Override
    public boolean isFinished(RemoteJobInformation remoteJobInformation) {
        return Files.exists(Paths.get(remoteJobInformation.getLogFile()));
    }

    @Override
    public boolean isFinished(RemoteJob remoteJob) {
        return !qstat("-xf " + remoteJob.getJobId(), QSTAT_EXIT_STATUS_ROW_NAME).isEmpty();
    }

    /** Returns the exit status from a PBS log file. */
    @Override
    public Integer exitStatus(RemoteJobInformation remoteJobInformation) {
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

    @Override
    /** Returns the exit comment from a PBS log, or null if there is no exit comment. */
    public String exitComment(RemoteJobInformation remoteJobInformation) {
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

    /**
     * Reconstitutes a {@link RemoteJobInformation} instance from an existing {@link RemoteJob}.
     * Allows monitoring to resume after an interruption in the supervisor.
     */
    @Override
    public RemoteJobInformation remoteJobInformation(RemoteJob remoteJob) {
        List<String> qstatOutput = qstat("-xf " + remoteJob.getJobId(), JOBNAME, OUTPUT_PATH);
        log.debug("job {}, qstat lines {}", remoteJob.getJobId(), qstatOutput.toString());
        Matcher m = JOBNAME_PATTERN.matcher(qstatOutput.get(0));
        String jobName = m.matches() ? m.group(1) : null;
        m = OUTPUT_PATH_PATTERN.matcher(qstatOutput.get(1));
        String logFile = m.matches() ? m.group(1) : null;
        if (StringUtils.isBlank(jobName) || StringUtils.isBlank(logFile)) {
            return null;
        }
        RemoteJobInformation remoteJobInformation = new RemoteJobInformation(logFile, jobName,
            remoteJob.getRemoteEnvironmentName());
        remoteJobInformation.setJobId(remoteJob.getJobId());
        return remoteJobInformation;
    }

    /**
     * Deletes all jobs for a given {@link PipelineTask}.
     */
    @Override
    public int deleteJobs(PipelineTask pipelineTask) {
        setPipelineTask(pipelineTask);
        Map<String, Long> jobIdByName = jobIdByName(pipelineTask);
        Set<Long> jobIds = new TreeSet<>(jobIdByName.values()); // Deterministic order
        return qdel(StringUtils.join(jobIds, " "));
    }

    @Override
    public double getUpdatedCostEstimate(RemoteJob remoteJob) {
        List<String> qstatOutput = qstat("-xf " + remoteJob.getJobId(), WALLTIME);
        if (qstatOutput.isEmpty()) {
            return remoteJob.getCostEstimate();
        }
        Matcher matcher = WALLTIME_PATTERN.matcher(qstatOutput.get(0));
        if (!matcher.matches()) {
            return remoteJob.getCostEstimate();
        }
        return TimeFormatter.timeStringHhMmSsToTimeInHours(matcher.group(1))
            * remoteJob.getCostFactor();
    }

    /**
     * Abstraction of the PBS qstat command. This runs the qstat command in an external process,
     * captures the output, and sorts through the output looking for the user-specified targets.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private List<String> qstat(String commandArgs, String... targets) {

        List<String> qstatResults = new ArrayList<>();
        try {
            ExternalProcess p = qstatExternalProcess(commandArgs);
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

    /** Abstraction of the qdel command. */
    protected int qdel(String qdelArgs) {
        ExternalProcess p = qdelExternalProcess(qdelArgs);
        log.info("Executing command {}", qdelArgs);
        return p.run(false, 0);
    }

    void createPbsLogDirectory() {
        try {
            Files.createDirectories(pbsLogDir());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    ExternalProcess qsubExternalProcess(CommandLine commandLine) {
        ExternalProcess p = ExternalProcess.stdoutLoggingExternalProcess(commandLine);

        // log to stdout/stderr & Log4j.
        p.logStdErr(true);
        return p;
    }

    List<CommandLine> getCommandLines() {
        return commandLines;
    }

    ExternalProcess qstatExternalProcess(String commandArgs) {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(QSTAT + commandArgs);
        p.logStdErr(false);
        p.logStdOut(false);
        p.writeStdErr(true);
        p.writeStdOut(true);
        return p;
    }

    ExternalProcess qdelExternalProcess(String commandArgs) {
        return ExternalProcess.simpleExternalProcess(QDEL + " " + commandArgs);
    }

    String user() {
        if (user == null) {
            ImmutableConfiguration config = ZiggyConfiguration.getInstance();
            String defaultUser = ZiggyConfiguration.getInstance()
                .getString(PropertyName.USER_NAME.property());
            user = config.getString(
                PropertyName.remoteUser(executionResources.getRemoteEnvironment().getName()),
                defaultUser);
        }
        return user;
    }

    String host() {
        if (host == null) {
            host = HostNameUtils.shortHostName();
        }
        return host;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    String algorithmLogFileSystemProperty(int jobIndex) {
        return ZiggyLog.algorithmLogFileSystemProperty(pipelineTask, jobIndex);
    }

    void setPipelineTask(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
        executionResources = pipelineTaskOperations().executionResources(pipelineTask);
    }

    /** {@link Callable} implementation for submission of PBS jobs. */
    private class CallableJobSubmitter implements Callable<RemoteJobInformation> {

        private final int jobIndex;

        public CallableJobSubmitter(int jobIndex) {
            this.jobIndex = jobIndex;
        }

        @Override
        public RemoteJobInformation call() {
            return submitOneJob(jobIndex);
        }
    }
}

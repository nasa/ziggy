package gov.nasa.ziggy.pipeline.definition;

import static com.google.common.base.Preconditions.checkState;
import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor.AlgorithmType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.HostNameUtils;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

/**
 * Represents a single pipeline unit of work Associated with a{@link PipelineInstance}, a
 * {@link PipelineDefinitionNode} (which is associated with a {@link PipelineModuleDefinition}), and
 * a {@link UnitOfWorkGenerator} that represents the unit of work.
 * <p>
 * Note that the {@link #equals(Object)} and {@link #hashCode()} methods are written in terms of
 * just the {@code id} field so this object should not be used in sets and maps until it has been
 * stored in the database
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_PipelineTask")
public class PipelineTask implements PipelineExecutionTime {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineTask.class);

    public enum State {
        /** Not yet started */
        INITIALIZED,

        /**
         * Task has been placed on the JMS queue, but not yet picked up by a worker.
         */
        SUBMITTED,

        /** Task is being processed by a worker */
        PROCESSING,

        /**
         * Task failed. Transition logic will not run (pipeline will stall). For tasks with
         * sub-tasks, this means that all sub-tasks failed.
         */
        ERROR,

        /** Task completed successfully. Transition logic will run. */
        COMPLETED,

        /**
         * Task contains sub-tasks and at least one sub-task failed and at least one sub-task
         * completed successfully
         */
        PARTIAL;

        public String toHtmlString() {
            String color = "black";

            switch (this) {
                case INITIALIZED:
                case SUBMITTED:
                    color = "black";
                    break;

                case PROCESSING:
                    color = "blue";
                    break;

                case COMPLETED:
                    color = "green";
                    break;

                case ERROR:
                case PARTIAL:
                    color = "red";
                    break;

                default:
            }

            return htmlBuilder().appendBoldColor(toString(), color).toString();
        }
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_PipelineTask_generator")
    @SequenceGenerator(name = "ziggy_PipelineTask_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineTask_sequence", allocationSize = 1)
    private Long id;

    /** Current state of this task */
    @Enumerated(EnumType.STRING)
    private State state = State.INITIALIZED;

    /** Current processing state of this task. */
    @Enumerated(EnumType.STRING)
    private ProcessingState processingState = ProcessingState.INITIALIZING;

    /** Total number of subtasks. */
    private int totalSubtaskCount;

    /** Number of completed subtasks. */
    private int completedSubtaskCount;

    /** Number of failed subtasks. */
    private int failedSubtaskCount;

    /**
     * Flag that indicates that this task was re-run from the console after an error
     */
    private boolean retry = false;

    /** Timestamp this task was created (either by launcher or transition logic) */
    private Date created = new Date();

    /** hostname of the worker that processed (or is processing) this task */
    private String workerHost;

    /** worker thread number that processed (or is processing) this task */
    private int workerThread;

    /** SVN revision of the build at the time this task was executed */
    private String softwareRevision;

    /** Timestamp that processing started on this task */
    private Date startProcessingTime = new Date(0);

    /** Timestamp that processing ended (success or failure) on this task */
    private Date endProcessingTime = new Date(0);

    /** Number of times this task failed and was rolled back */
    private int failureCount = 0;

    /** Count of the number of automatic resubmits performed for this task. */
    private int autoResubmitCount = 0;

    /** Indicates whether the task has been submitted for remote execution. */
    @Enumerated(EnumType.STRING)
    private AlgorithmType processingMode;

    @ManyToOne
    private PipelineInstance pipelineInstance = null;

    @ManyToOne
    private PipelineInstanceNode pipelineInstanceNode = null;

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_PipelineTask_uowTaskParameters")
    private Set<TypedParameter> uowTaskParameters;

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineTask_summaryMetrics")
    private List<PipelineTaskMetrics> summaryMetrics = new ArrayList<>();

    @ElementCollection
    @OrderColumn(name = "idx")
    @JoinTable(name = "ziggy_PipelineTask_execLog")
    private List<TaskExecutionLog> execLog = new ArrayList<>();

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineTask_producerTaskIds")
    private Set<Long> producerTaskIds = new TreeSet<>();

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineTask_remoteJobs")
    private Set<RemoteJob> remoteJobs = new HashSet<>();

    private int taskLogIndex = 0;

    private long priorProcessingExecutionTimeMillis = -1;

    private long currentExecutionStartTimeMillis;

    @Transient
    private int maxFailedSubtaskCount;

    @Transient
    private int maxAutoResubmits;

    /**
     * Required by Hibernate
     */
    public PipelineTask() {
    }

    public PipelineTask(PipelineInstance pipelineInstance,
        PipelineInstanceNode pipelineInstanceNode) {
        this.pipelineInstance = pipelineInstance;
        this.pipelineInstanceNode = pipelineInstanceNode;
    }

    /**
     * Get {@link Parameters} from either the pipeline or module parameters for the specified class.
     *
     * @throws PipelineException if the parameters are not defined as either pipeline parameters or
     * module parameters, or if they are defined at both levels.
     */
    public <T extends Parameters> T getParameters(Class<T> parametersClass) {
        return getParameters(parametersClass, true);
    }

    /**
     * Get {@link Parameters} from either the pipeline or module parameters for the specified class.
     *
     * @throws PipelineException if {@code throwIfMissing} is {@code true} and the parameters are
     * not defined as either pipeline parameters or module parameters, or if they are defined at
     * both levels.
     */
    @SuppressWarnings("unchecked")
    public <T extends Parameters> T getParameters(Class<T> parametersClass,
        boolean throwIfMissing) {
        ParameterSet parameterSet = getParameterSet(parametersClass, throwIfMissing);
        return parameterSet == null ? null : (T) parameterSet.parametersInstance();
    }

    /**
     * Get {@link ParameterSet} from either the pipeline or module parameters for the specified
     * class.
     *
     * @throws PipelineException if the parameters are not defined as either pipeline parameters or
     * module parameters, or if they are defined at both levels.
     */
    public <T extends Parameters> ParameterSet getParameterSet(Class<T> parametersClass) {
        return getParameterSet(parametersClass, true);
    }

    /**
     * Returns the full {@link Map} of pipeline-level {@link ParameterSet} instances. This is mainly
     * used to force Hibernate to lazy-instantiate the parameters.
     */
    public Map<ClassWrapper<ParametersInterface>, ParameterSet> getPipelineParameterSets() {
        return pipelineInstance.getPipelineParameterSets();
    }

    /**
     * Returns the full {@link Map} of module-level {@link ParameterSet} instances. This is mainly
     * used to force Hibernate to lazy-instantiate the parameters.
     */
    public Map<ClassWrapper<ParametersInterface>, ParameterSet> getModuleParameterSets() {
        return pipelineInstanceNode.getModuleParameterSets();
    }

    /**
     * Get {@link ParameterSet} from either the pipeline or module parameters for the specified
     * class.
     *
     * @throws PipelineException if {@code throwIfMissing} is {@code true} and the parameters are
     * not defined as either pipeline parameters or module parameters, or if they are defined at
     * both levels.
     */
    public <T extends Parameters> ParameterSet getParameterSet(Class<T> parametersClass,
        boolean throwIfMissing) {

        ParameterSet pipelineParamSet = pipelineInstance.getPipelineParameterSet(parametersClass);
        ParameterSet moduleParamSet = pipelineInstanceNode.getModuleParameterSet(parametersClass);

        if (pipelineParamSet == null && moduleParamSet == null) {
            String errMsg = "Parameters for class: " + parametersClass
                + " not found in either pipeline parameters or module parameters";

            if (throwIfMissing) {
                throw new PipelineException(errMsg);
            }
            return null;
        }
        if (pipelineParamSet != null && moduleParamSet != null) {
            throw new PipelineException("Parameters for class: " + parametersClass
                + " found in both pipeline parameters and module parameters");
        }
        if (moduleParamSet != null) {
            return moduleParamSet;
        }
        return pipelineParamSet;
    }

    /**
     * Conveninence method for getting the externalId for a model for this pipeline task.
     *
     * @return
     */
    public int getModelExternalId(String modelType) {
        ModelRegistry modelRegistry = getPipelineInstance().getModelRegistry();
        ModelMetadata modelMetadata = modelRegistry.getMetadataForType(modelType);
        if (modelMetadata == null) {
            throw new PipelineException("No model metadata found for modelType=" + modelType);
        }
        return Integer.parseInt(modelMetadata.getModelRevision());
    }

    /**
     * A human readable description of this task and its parameters.
     *
     * @throws PipelineException
     */
    public String prettyPrint() {
        PipelineModuleDefinition moduleDefinition = pipelineInstanceNode
            .getPipelineModuleDefinition();

        StringBuilder bldr = new StringBuilder();
        bldr.append("TaskId: ")
            .append(getId())
            .append(" ")
            .append("Module Software Revision: ")
            .append(getSoftwareRevision())
            .append(" ")
            .append(moduleDefinition.getName())
            .append(" ")
            .append(" UoW: ")
            .append(uowTaskInstance().briefState())
            .append(" ");

        Collection<ParameterSet> setOfParameterSets = pipelineInstanceNode.getModuleParameterSets()
            .values();

        for (ParameterSet pset : setOfParameterSets) {
            bldr.append('[').append(pset.getDescription()).append(" ");
            bldr.append(pset.getVersion()).append(" ");
            Set<TypedParameter> properties = pset.getTypedParameters();
            for (TypedParameter property : properties) {
                bldr.append(property.getName()).append("=").append(property.getValue()).append(" ");
            }
            bldr.append(']');
        }

        return bldr.toString();
    }

    public String getModuleName() {
        return getPipelineInstanceNode().getPipelineModuleDefinition().getName();
    }

    public PipelineModule getModuleImplementation() {
        return getModuleImplementation(RunMode.STANDARD);
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public PipelineModule getModuleImplementation(RunMode runMode) {
        ClassWrapper<PipelineModule> moduleWrapper = pipelineInstanceNode
            .getPipelineModuleDefinition()
            .getPipelineModuleClass();
        try {
            return moduleWrapper.constructor(PipelineTask.class, RunMode.class)
                .newInstance(this, runMode);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
            // Can never occur. The PipelineModule has a constructor that takes PipelineTask
            // and RunMode as arguments.
            throw new AssertionError(e);
        }
    }

    public PipelineDefinitionNode pipelineDefinitionNode() {
        return pipelineInstanceNode.getPipelineDefinitionNode();
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public PipelineInstance getPipelineInstance() {
        return pipelineInstance;
    }

    public void setPipelineInstance(PipelineInstance instance) {
        pipelineInstance = instance;
    }

    public long pipelineInstanceId() {
        return pipelineInstance.getId();
    }

    public PipelineDefinition pipelineDefinition() {
        return pipelineInstance.getPipelineDefinition();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Set<TypedParameter> getUowTaskParameters() {
        return uowTaskParameters;
    }

    public void setUowTaskParameters(Set<TypedParameter> uowTaskParameters) {
        this.uowTaskParameters = uowTaskParameters;
    }

    @Override
    public String toString() {
        return "PipelineTask [id=" + id + ", state=" + state + ", uowTask="
            + uowTaskInstance().briefState() + "]";
    }

    public UnitOfWork uowTaskInstance() {
        UnitOfWork uow = new UnitOfWork();
        uow.setParameters(uowTaskParameters);
        return uow;
    }

    public State getState() {
        return state;
    }

    /**
     * Use of this method is not recommended. Use
     * {@link PipelineOperations#setTaskState(PipelineTask, gov.nasa.ziggy.pipeline.definition.PipelineTask.State)}
     * instead.
     *
     * @param state
     */
    public void setState(State state) {
        this.state = state;
    }

    public ProcessingState getProcessingState() {
        return processingState;
    }

    public void setProcessingState(ProcessingState processingState) {
        this.processingState = processingState;
    }

    public int getTotalSubtaskCount() {
        return totalSubtaskCount;
    }

    public void setTotalSubtaskCount(int totalSubtaskCount) {
        this.totalSubtaskCount = totalSubtaskCount;
    }

    public int getCompletedSubtaskCount() {
        return completedSubtaskCount;
    }

    public void setCompletedSubtaskCount(int completedSubtaskCount) {
        this.completedSubtaskCount = completedSubtaskCount;
    }

    public int getFailedSubtaskCount() {
        return failedSubtaskCount;
    }

    public void setFailedSubtaskCount(int failedSubtaskCount) {
        this.failedSubtaskCount = failedSubtaskCount;
    }

    @Override
    public Date getEndProcessingTime() {
        return endProcessingTime;
    }

    @Override
    public void setEndProcessingTime(Date endProcessingTime) {
        this.endProcessingTime = endProcessingTime;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void setFailureCount(int failureCount) {
        this.failureCount = failureCount;
    }

    public void incrementFailureCount() {
        failureCount++;
    }

    @Override
    public Date getStartProcessingTime() {
        return startProcessingTime;
    }

    @Override
    public void setStartProcessingTime(Date startProcessingTime) {
        this.startProcessingTime = startProcessingTime;
    }

    public String getWorkerName() {
        if (workerHost != null && workerHost.length() > 0) {
            String host = HostNameUtils.callerHostNameOrLocalhost(workerHost);
            return host + ":" + workerThread;
        }
        return "-";
    }

    public PipelineInstanceNode getPipelineInstanceNode() {
        return pipelineInstanceNode;
    }

    public int exeTimeoutSeconds() {
        return getPipelineInstanceNode().getPipelineModuleDefinition().getExeTimeoutSecs();
    }

    public long pipelineInstanceNodeId() {
        return getPipelineInstanceNode().getId();
    }

    public String getSoftwareRevision() {
        return softwareRevision;
    }

    public void setSoftwareRevision(String softwareRevision) {
        this.softwareRevision = softwareRevision;
    }

    public String getWorkerHost() {
        return workerHost;
    }

    public void setWorkerHost(String workerHost) {
        this.workerHost = workerHost;
    }

    public int getWorkerThread() {
        return workerThread;
    }

    public void setWorkerThread(int workerThread) {
        this.workerThread = workerThread;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }

    public List<PipelineTaskMetrics> getSummaryMetrics() {
        return summaryMetrics;
    }

    public void setSummaryMetrics(List<PipelineTaskMetrics> summaryMetrics) {
        this.summaryMetrics = summaryMetrics;
    }

    public List<TaskExecutionLog> getExecLog() {
        return execLog;
    }

    public void setExecLog(List<TaskExecutionLog> execLog) {
        this.execLog = execLog;
    }

    public void addProducerTaskIds(Collection<Long> producerTaskIds) {
        this.producerTaskIds.addAll(producerTaskIds);
    }

    public void addProducerTaskId(long producerTaskId) {
        producerTaskIds.add(producerTaskId);
    }

    public Set<Long> getProducerTaskIds() {
        return producerTaskIds;
    }

    public void clearProducerTaskIds() {
        producerTaskIds = new TreeSet<>();
    }

    public void setProducerTaskIds(Collection<Long> producerTaskIds) {
        clearProducerTaskIds();
        if (producerTaskIds != null) {
            addProducerTaskIds(producerTaskIds);
        }
    }

    public void setTaskLogIndex(int taskLogIndex) {
        this.taskLogIndex = taskLogIndex;
    }

    public int getTaskLogIndex() {
        return taskLogIndex;
    }

    public void incrementTaskLogIndex() {
        taskLogIndex++;
    }

    public String taskBaseName() {
        return taskBaseName(pipelineInstance.getId(), getId(), getModuleName());
    }

    public static String taskBaseName(long instanceId, long taskId, String moduleName) {
        return baseNamePrefix(instanceId, taskId) + "-" + moduleName;
    }

    public static String baseNamePrefix(long instanceId, long taskId) {
        return Long.toString(instanceId) + "-" + Long.toString(taskId);
    }

    /**
     * Returns the label for a task that is used in some UI displays.
     */
    public String taskLabelText() {

        return htmlBuilder().appendBold("ID: ")
            .append(getPipelineInstance().getId())
            .append(":")
            .append(id)
            .appendBold(" WORKER: ")
            .append(getWorkerName())
            .appendBold(" TASK: ")
            .append(getModuleName())
            .append(" [")
            .append(uowTaskInstance().briefState())
            .append("] ")
            .appendBoldColor(state.toString(), state == State.ERROR ? "red" : "green")
            .append(" ")
            .appendItalic(elapsedTime())
            .toString();
    }

    /**
     * Produces a file name for log files of the following form: {@code
     * <instanceId>-<taskId>-<moduleName>.<jobIndex>-<taskLogIndex>.log
     * }
     *
     * @param jobIndex index for the current job. Each pipeline task's algorithm execution can be
     * performed across multiple independent jobs; this index identifies a specific job out of the
     * set that are running for the current task.
     */
    public String logFilename(int jobIndex) {
        return taskBaseName() + "." + jobIndex + "-" + taskLogIndex + ".log";
    }

    /**
     * For TEST use only
     */
    public void setPipelineInstanceNode(PipelineInstanceNode pipelineInstanceNode) {
        this.pipelineInstanceNode = pipelineInstanceNode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineTask other = (PipelineTask) obj;
        return Objects.equals(id, other.id);
    }

    public static final class TaskBaseNameMatcher {
        public static final int INSTANCE_ID_GROUP = 1;
        public static final int TASK_ID_GROUP = 2;
        public static final int MODULE_NAME_GROUP = 3;
        public static final int JOB_INDEX_GROUP = 5;
        public static final String regex = "(\\d+)-(\\d+)-([^\\s\\.]+){1}?(\\.(\\d+))?";
        public static final Pattern pattern = Pattern.compile(regex);

        private long instanceId;
        private long taskId;
        private String moduleName;
        private boolean hasJobIndex;
        private int jobIndex;
        private boolean matches;
        private String baseName;

        public TaskBaseNameMatcher(String baseName) {
            this.baseName = baseName;
            Matcher matcher = pattern.matcher(baseName);
            matches = matcher.matches();
            if (matches) {
                instanceId = Long.parseLong(matcher.group(INSTANCE_ID_GROUP));
                taskId = Long.parseLong(matcher.group(TASK_ID_GROUP));
                moduleName = matcher.group(MODULE_NAME_GROUP);
                hasJobIndex = matcher.group(JOB_INDEX_GROUP) != null;
                if (hasJobIndex) {
                    jobIndex = Integer.parseInt(matcher.group(JOB_INDEX_GROUP));
                }
            }
        }

        public boolean matches() {
            return matches;
        }

        private void checkMatch() {
            checkState(matches, "String " + baseName + " does not match task base name pattern");
        }

        private void checkHasJobIndex() {
            checkState(matches, "String " + baseName + " does not have a job index");
        }

        public long instanceId() {
            checkMatch();
            return instanceId;
        }

        public long taskId() {
            checkMatch();
            return taskId;
        }

        public String moduleName() {
            checkMatch();
            return moduleName;
        }

        public boolean hasJobIndex() {
            return hasJobIndex;
        }

        public int jobIndex() {
            checkMatch();
            checkHasJobIndex();
            return jobIndex;
        }
    }

    @Override
    public void setPriorProcessingExecutionTimeMillis(long priorProcessingExecutionTimeMillis) {
        this.priorProcessingExecutionTimeMillis = priorProcessingExecutionTimeMillis;
    }

    @Override
    public long getPriorProcessingExecutionTimeMillis() {
        return priorProcessingExecutionTimeMillis;
    }

    @Override
    public void setCurrentExecutionStartTimeMillis(long currentExecutionStartTimeMillis) {
        this.currentExecutionStartTimeMillis = currentExecutionStartTimeMillis;
    }

    @Override
    public long getCurrentExecutionStartTimeMillis() {
        return currentExecutionStartTimeMillis;
    }

    public int getMaxFailedSubtaskCount() {
        return maxFailedSubtaskCount;
    }

    public void setMaxFailedSubtaskCount(int maxFailedSubtaskCount) {
        this.maxFailedSubtaskCount = maxFailedSubtaskCount;
    }

    public int getMaxAutoResubmits() {
        return maxAutoResubmits;
    }

    public void setMaxAutoResubmits(int maxAutoResubmits) {
        this.maxAutoResubmits = maxAutoResubmits;
    }

    public Set<RemoteJob> getRemoteJobs() {
        return remoteJobs;
    }

    public void setRemoteJobs(Set<RemoteJob> remoteJobs) {
        this.remoteJobs = remoteJobs;
    }

    public boolean isRemoteExecution() {
        return processingMode.equals(AlgorithmType.REMOTE);
    }

    public void setRemoteExecution(boolean remoteExecution) {
        processingMode = remoteExecution ? AlgorithmType.REMOTE : AlgorithmType.LOCAL;
    }

    public AlgorithmType getProcessingMode() {
        return processingMode;
    }

    public void setProcessingMode(AlgorithmType mode) {
        processingMode = mode;
    }

    public int getAutoResubmitCount() {
        return autoResubmitCount;
    }

    public void setAutoResubmitCount(int autoResubmitCount) {
        this.autoResubmitCount = autoResubmitCount;
    }

    public void incrementAutoResubmitCount() {
        autoResubmitCount++;
    }

    public void resetAutoResubmitCount() {
        autoResubmitCount = 0;
    }

    /**
     * Starts execution clock for {@link PipelineTask} by first using the default method from
     * {@link PipelineExecutionTime}, then starting the clock for the {@link PipelineInstance} as
     * well. Users are generally advised not to call this method directly, as the clock is started
     * and stopped at appropriate times when a {@link PipelineOperations} instance is used to set a
     * task's state.
     */
    @Override
    public void startExecutionClock() {
        PipelineExecutionTime.super.startExecutionClock();
        getPipelineInstance().startExecutionClock();
    }

    public double costEstimate() {
        double estimate = 0;
        for (RemoteJob job : remoteJobs) {
            estimate += job.getCostEstimate();
        }
        return estimate;
    }

    /**
     * Convenience class that provides read-only copies of the current values of the processing
     * state and subtask count fields of a {@link PipelineTask}.
     *
     * @author PT
     */
    public static class ProcessingSummary {

        private final long id;
        private final int totalSubtaskCount;
        private final int completedSubtaskCount;
        private final int failedSubtaskCount;
        private final ProcessingState processingState;

        public ProcessingSummary(PipelineTask pipelineTask) {
            id = pipelineTask.getId();
            totalSubtaskCount = pipelineTask.getTotalSubtaskCount();
            completedSubtaskCount = pipelineTask.getCompletedSubtaskCount();
            failedSubtaskCount = pipelineTask.getFailedSubtaskCount();
            processingState = pipelineTask.getProcessingState();
        }

        public long getId() {
            return id;
        }

        public int getTotalSubtaskCount() {
            return totalSubtaskCount;
        }

        public int getCompletedSubtaskCount() {
            return completedSubtaskCount;
        }

        public int getFailedSubtaskCount() {
            return failedSubtaskCount;
        }

        public ProcessingState getProcessingState() {
            return processingState;
        }

        public String processingStateShortLabel() {
            String pState = "?";
            if (processingState != null) {
                pState = processingState.shortName();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(pState);
            sb.append(" (");
            sb.append(totalSubtaskCount);
            sb.append(" / ");
            sb.append(completedSubtaskCount);
            sb.append(" / ");
            sb.append(failedSubtaskCount);
            sb.append(")");

            return sb.toString();
        }
    }
}

package gov.nasa.ziggy.services.events;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.InstanceAndTasks;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionName;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.alert.AlertService.Severity;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.util.Iso8601Formatter;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Handles a single type of Ziggy event. Ziggy events are a system that automatically starts
 * execution of a pipeline in response to the appearance of a specified file on the file system.
 * More specifically, the {@link ZiggyEventHandler} watches for a ready-indicator file to appear in
 * the watched directory; at that time, the pipeline begins execution. Once pipeline execution has
 * started, the ready-indicator file is removed and the event handler returns to watching for
 * events.
 *
 * @author PT
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_ZIGGY_EVENT_HANDLER")
public class ZiggyEventHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ZiggyEventHandler.class);

    // The pattern for the name of a ready file. The pattern is:
    //
    // <label>.READY.<name>.<count>,
    //
    // where <count> is the total number of ready files with a given <name>, and
    // <label> is a string that is unique among all the ready files for a given <name>.
    // When the number of ready files for a given <name> matches the value of <count>,
    // the event represented by <name> is ready to be acted upon.
    private static final Pattern READY_FILE_NAME = Pattern
        .compile("(\\S+)\\.READY\\.(\\S+)\\.([0-9]+)");
    private static final int LABEL_GROUP_NUMBER = 1;
    private static final int NAME_GROUP_NUMBER = 2;
    private static final int COUNT_GROUP_NUMBER = 3;
    static final String XML_SCHEMA_FILE_NAME = "pipeline-events.xsd";
    private static final long READY_FILE_CHECK_INTERVAL_MILLIS = 10_000;

    // Special case value for the label that indicates that the user actually wants the label
    // to be empty.
    private static final String EMPTY_LABEL_VALUE = "null";

    @Id
    @XmlAttribute(required = true)
    private String name = "";

    /**
     * Determines whether the {@link ZiggyEventHandler} should be enabled when the cluster is
     * started. By default, event handlers must be manually enabled; this field signals to the
     * worker process that, upon startup, this event handler should start.
     */
    @XmlAttribute
    private boolean enableOnClusterStart;

    /**
     * Directory to watch for the ready-indicator file. This can include property names that must be
     * interpolated by the configuration system (i.e., a value of "${ziggy.data.dir}/subdir" is
     * permitted, so long as ziggy.data.dir is the name of a property known to the configuration
     * system).
     */
    @XmlAttribute(required = true)
    private String directory;

    /**
     * Pipeline to start when an event is detected.
     */
    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(PipelineDefinitionName.PipelineNameAdapter.class)
    @ManyToOne(targetEntity = PipelineDefinitionName.class, fetch = FetchType.EAGER)
    private PipelineDefinitionName pipelineName;

    @Transient
    private PipelineOperations pipelineOperations;

    /**
     * {@link ScheduledExecutorService} that provides a thread for the ready-indicator check.
     */
    @Transient
    private ScheduledExecutorService watcherThread;

    public ZiggyEventHandler() {
        ZiggyShutdownHook.addShutdownHook(() -> stop());
    }

    /**
     * Starts watching for the specified event. Specifically, this method submits the {@link #run()}
     * method to the {@link ScheduledExecutorService}.
     */
    public void start() {
        watcherThread = new ScheduledThreadPoolExecutor(1);
        watcherThread.scheduleAtFixedRate(this, 0L, readyFileCheckIntervalMillis(),
            TimeUnit.MILLISECONDS);
    }

    /**
     * Stops watching for the ready-indicator file.
     */
    public void stop() {
        if (watcherThread != null) {
            watcherThread.shutdownNow();
            watcherThread = null;
        }
    }

    public boolean isRunning() {
        return watcherThread != null;
    }

    /**
     * Checks for the ready-indicator file and starts the pipeline if it is present.
     */
    @Override
    public void run() {

        try {
            for (ReadyFile readyFile : readyFilesForExecution()) {
                startPipeline(readyFile);
                readyFile.delete(interpolatedDirectory());
            }
        } catch (Exception e) {
            log.error("ZiggyEventHandler " + name + " disabled due to exception", e);
            alertService().generateAndBroadcastAlert("Event handler " + name, 0, Severity.WARNING,
                "Event handler shut down due to exception");
            stop();
        }
    }

    /**
     * Finds any ready files that indicate a readiness for execution (i.e., all the ready files for
     * a given name have appeared). Package scope so that it can be used to introduce an exception
     * during unit tests.
     */
    Set<ReadyFile> readyFilesForExecution() {

        Set<ReadyFile> readyFilesForExecution = new HashSet<>();
        File[] readyFilesInWatchedDirectory = interpolatedDirectory().toFile()
            .listFiles(
                (FilenameFilter) (dir, filename) -> READY_FILE_NAME.matcher(filename).matches());
        if (readyFilesInWatchedDirectory == null || readyFilesInWatchedDirectory.length == 0) {
            return readyFilesForExecution;
        }

        Map<String, ReadyFile> readyFilesByName = new HashMap<>();
        for (File readyFileInWatchedDirectory : readyFilesInWatchedDirectory) {
            Matcher fileMatch = READY_FILE_NAME.matcher(readyFileInWatchedDirectory.getName());
            fileMatch.matches();
            String readyFileName = fileMatch.group(NAME_GROUP_NUMBER);
            ReadyFile readyFile = readyFilesByName.get(readyFileName);
            if (readyFile == null) {
                readyFilesByName.put(readyFileName, new ReadyFile(fileMatch));
            } else {
                readyFile.add(fileMatch);
            }
        }
        for (ReadyFile readyFile : readyFilesByName.values()) {
            if (readyFile.isReady()) {
                readyFilesForExecution.add(readyFile);
            }
        }
        return readyFilesForExecution;
    }

    /**
     * Starts the pipeline in response to an event, and creates a {@link ZiggyEvent} instance in the
     * database.
     */
    private void startPipeline(ReadyFile readyFile) {

        log.info("Event handler " + name + " detected event");
        log.info(
            "Event name \"" + readyFile.getName() + "\" has " + readyFile.labelCount() + " labels");
        log.debug("Event handler labels: " + readyFile.labels());
        log.info("Event handler " + name + " starting pipeline " + pipelineName.getName() + "...");

        // Start by saving the event labels as a parameter set.
        String paramSetName = (String) DatabaseTransactionFactory.performTransaction(() -> {

            String parameterSetName = name + " " + readyFile.getName();
            String parameterSetDescription = "Created by event handler " + name + " @ "
                + new Date();
            ZiggyEventLabels eventLabels = null;
            ParameterSet paramSet = new ParameterSetCrud()
                .retrieveLatestVersionForName(parameterSetName);
            if (paramSet != null) {
                eventLabels = (ZiggyEventLabels) paramSet.getParameters().getInstance();
                eventLabels.setEventName(readyFile.getName());
                eventLabels.setEventLabels(readyFile.labelsArray());
                pipelineOperations().updateParameterSet(paramSet, eventLabels,
                    parameterSetDescription, true);
            } else {
                paramSet = new ParameterSet(parameterSetName);
                paramSet.setDescription(parameterSetDescription);
                eventLabels = new ZiggyEventLabels();
                eventLabels.setEventHandlerName(name);
                eventLabels.setEventName(readyFile.getName());
                eventLabels.setEventLabels(readyFile.labelsArray());
                paramSet.setParameters(new BeanWrapper<Parameters>(eventLabels));
                new ParameterSetCrud().create(paramSet);
            }
            return parameterSetName;
        });

        // Create a new pipeline instance that includes the event handler labels parameter set.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                .retrieveLatestVersionForName(pipelineName);
            InstanceAndTasks instanceInfo = pipelineOperations().fireTrigger(pipelineDefinition,
                instanceName(), null, null, paramSetName);
            sendWorkerMessageForTasks(instanceInfo.getPipelineTasks());
            final ZiggyEvent event = new ZiggyEvent(name, pipelineName,
                instanceInfo.getPipelineInstance().getId());
            new ZiggyEventCrud().create(event);
            return null;
        });
        log.info(
            "Event handler " + name + " starting pipeline " + pipelineName.getName() + "...done");
    }

    /**
     * Sends task messages to the worker. Package scoped so it can be mocked out.
     */
    void sendWorkerMessageForTasks(List<PipelineTask> pipelineTasks) {
        for (PipelineTask pipelineTask : pipelineTasks) {
            pipelineOperations().sendWorkerMessageForTask(pipelineTask);
        }
    }

    /**
     * Toggles the state of the {@link ZiggyEventHandler} from enabled to disabled or vice-versa.
     */
    public void toggleStatus() {
        log.debug("Toggling state of event handler \"" + name + "\"");
        if (watcherThread == null) { // indicates disabled
            start();
        } else {
            stop();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnableOnClusterStart() {
        return enableOnClusterStart;
    }

    public void setEnableOnClusterStart(boolean enableOnPipelineStart) {
        enableOnClusterStart = enableOnPipelineStart;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public PipelineDefinitionName getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(PipelineDefinitionName pipelineName) {
        this.pipelineName = pipelineName;
    }

    /**
     * Returns an instance of {@link PipelineOperations}, which allows a mocked instance to be
     * substituted for test purposes. Package scope for tests.
     */
    PipelineOperations pipelineOperations() {
        if (pipelineOperations == null) {
            pipelineOperations = new PipelineOperations();
        }
        return pipelineOperations;
    }

    /**
     * Returns the current instance of {@link AlertService}, which allows a mocked instance to be
     * substituted for test purposes. Package scope for tests.
     */
    AlertService alertService() {
        return AlertService.getInstance();
    }

    /**
     * Returns the desired interval between tests for the ready-indicator file. Package scope so
     * that it can be mocked out with a different value during tests.
     */
    long readyFileCheckIntervalMillis() {
        return READY_FILE_CHECK_INTERVAL_MILLIS;
    }

    /**
     * Returns an instance name that combines the name of the {@link ZiggyEventHandler} with a
     * timestamp. The instance name is provided by a method which allows a fixed name to be
     * specified for test purposes. Package scope for tests.
     */
    String instanceName() {
        return name + "-" + Iso8601Formatter.dateTimeLocalFormatter().format(new Date());
    }

    Path interpolatedDirectory() {
        return Paths.get((String) PropertyConverter.interpolate(directory, configuration()));
    }

    /**
     * Returns the Ziggy configuration, cast back to its actual class
     * ({@link CompositeConfiguration}). Because this does not work correctly in test, the method is
     * broken out so that it can be mocked in test cases.
     */
    CompositeConfiguration configuration() {
        return (CompositeConfiguration) ZiggyConfiguration.unsynchronizedInstance();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ZiggyEventHandler other = (ZiggyEventHandler) obj;
        return Objects.equals(name, other.name);
    }

    /**
     * A digest of the contents of {@link ZiggyEventHandler} in which only information of interest
     * to the user of a pipeline console is represented, and all information is represented by
     * fields that implement {@link Serializable}. This allows the
     * {@link ZiggyEventHandlerInfoForDisplay} to be transported as part of a message in RMI.
     *
     * @author PT
     */
    public static class ZiggyEventHandlerInfoForDisplay implements Serializable {

        private static final long serialVersionUID = 20220707L;

        private String name;
        private boolean enabled;
        private String directory;
        private String pipelineName;

        public ZiggyEventHandlerInfoForDisplay(ZiggyEventHandler eventHandler) {
            name = eventHandler.getName();
            enabled = eventHandler.isRunning();
            directory = eventHandler.interpolatedDirectory().toString();
            pipelineName = eventHandler.getPipelineName().getName();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }

        public String getPipelineName() {
            return pipelineName;
        }

        public void setPipelineName(String pipelineName) {
            this.pipelineName = pipelineName;
        }

    }

    /**
     * Stores information on the ready files associated with a given name.
     *
     * @author PT
     */
    private static class ReadyFile {

        private String name;
        private int count;
        private Set<String> labels = new HashSet<>();
        private Set<String> filenames = new HashSet<>();

        public ReadyFile(Matcher matcher) {
            name = matcher.group(NAME_GROUP_NUMBER);
            count = Integer.parseInt(matcher.group(COUNT_GROUP_NUMBER));
            add(matcher);
        }

        public void add(Matcher matcher) {
            if (!matcher.group(LABEL_GROUP_NUMBER).equals(EMPTY_LABEL_VALUE)) {
                labels.add(matcher.group(LABEL_GROUP_NUMBER));
            }
            filenames.add(matcher.group(0));
        }

        public boolean isReady() {
            return count == filenames.size();
        }

        public void delete(Path directory) throws IOException {
            for (String filename : filenames) {
                Files.delete(directory.resolve(filename));
            }
        }

        public String getName() {
            return name;
        }

        public int labelCount() {
            return labels.size();
        }

        public String labels() {
            return labels.toString();
        }

        public String[] labelsArray() {
            return labels.toArray(new String[0]);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ReadyFile other = (ReadyFile) obj;
            return Objects.equals(name, other.name);
        }

    }

}

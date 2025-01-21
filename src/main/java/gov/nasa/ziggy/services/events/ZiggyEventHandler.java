package gov.nasa.ziggy.services.events;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.EventHandlerToggleStateRequest;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;

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
@Table(name = "ziggy_EventHandler")
public class ZiggyEventHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ZiggyEventHandler.class);

    // The pattern for the name of a ready file. The pattern is:
    //
    // <label>.READY.<name>.<count> ,
    //
    // where <count> is the total number of ready files with a given <name>, and
    // <label> is a string that is unique among all the ready files for a given <name>.
    // When the number of ready files for a given <name> matches the value of <count>,
    // the event represented by <name> is ready to be acted upon.
    // Note that in the event that there is a single ready file, the user can optionally
    // decide to omit the label, in which case the pattern becomes:
    //
    // READY.<name>.<count> .
    //
    // Note that the first group includes the dot between the label and READY portions
    // of the file name.
    private static final Pattern READY_FILE_NAME = Pattern
        .compile("(\\S+\\.)?READY\\.(\\S+)\\.([0-9]+)");
    private static final int LABEL_GROUP_NUMBER = 1;
    private static final int NAME_GROUP_NUMBER = 2;
    private static final int COUNT_GROUP_NUMBER = 3;
    static final String XML_SCHEMA_FILE_NAME = "pipeline-events.xsd";
    private static final long READY_FILE_CHECK_INTERVAL_MILLIS = 10_000;

    @Transient
    private PipelineExecutor pipelineExecutor = new PipelineExecutor();

    @Id
    @XmlAttribute(required = true)
    private String name = "";

    /**
     * Determines whether the {@link ZiggyEventHandler} should be enabled when the cluster is
     * started. By default, event handlers must be manually enabled; this field signals to the
     * supervisor process that, upon startup, this event handler should start.
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
    private String pipelineName;

    @Transient
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();

    @Transient
    private ZiggyEventOperations ziggyEventOperations = new ZiggyEventOperations();
    /**
     * {@link ScheduledExecutorService} that provides a thread for the ready-indicator check.
     */
    @Transient
    private ScheduledExecutorService watcherThread;

    public ZiggyEventHandler() {
        ZiggyShutdownHook.addShutdownHook(this::stop);
        ZiggyMessenger.subscribe(EventHandlerToggleStateRequest.class, message -> {
            EventHandlerToggleStateRequest request = message;
            if (request.getHandlerName().equals(name)) {
                toggleStatus();
            }
        });
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
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_IN_RUNNABLE)
    public void run() {

        try {
            for (ReadyFile readyFile : readyFilesForExecution()) {
                startPipeline(readyFile);
                readyFile.delete(interpolatedDirectory());
            }
        } catch (Exception e) {
            // If we got here, something bad happened either when looking for ready files
            // or when starting the pipeline that responds to the ready file. In either case,
            // we don't want the event handler to try again in a few seconds, but we also
            // don't want the event handler infrastructure itself to fail as a result. For
            // this reason, we catch Exception here (to include both runtime exceptions and
            // unexpected checked exceptions), and then stop this particular event handler.
            // TODO Use AlertService.DEFAULT_TASK here instead?
            log.error("ZiggyEventHandler {} disabled due to exception", name, e);
            alertService().generateAndBroadcastAlert("Event handler " + name,
                new PipelineTask(null, null, null), Severity.WARNING,
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

        log.info("Event handler {} detected event", name);
        log.info("Event {} has {} labels", readyFile.getName(), readyFile.labelCount());
        log.debug("Event handler labels are {}", readyFile.labels());
        log.info("Event handler {} starting pipeline {}", name, pipelineName);

        // Create a new pipeline instance that includes the event handler labels.
        PipelineDefinition pipelineDefinition = pipelineDefinitionOperations()
            .pipelineDefinition(pipelineName);
        PipelineInstance pipelineInstance = pipelineExecutor().launch(pipelineDefinition, null,
            null, null, readyFile.getLabels());
        ZiggyMessenger.publish(new PipelineInstanceStartedMessage());
        ziggyEventOperations().newZiggyEvent(name, pipelineName, pipelineInstance.getId(),
            readyFile.getLabels());
        log.info("Event handler {} starting pipeline {}...done", name, pipelineName);
    }

    /**
     * Toggles the state of the {@link ZiggyEventHandler} from enabled to disabled or vice-versa.
     */
    public void toggleStatus() {
        log.debug("Toggling state of event handler {}", name);
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

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    PipelineExecutor pipelineExecutor() {
        return pipelineExecutor;
    }

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    ZiggyEventOperations ziggyEventOperations() {
        return ziggyEventOperations;
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

    private Path interpolatedDirectory() {
        return Paths.get((String) ZiggyConfiguration.interpolate(directory));
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

        private static final long serialVersionUID = 20230511L;

        private String name;
        private boolean enabled;
        private String directory;
        private String pipelineName;

        public ZiggyEventHandlerInfoForDisplay(ZiggyEventHandler eventHandler) {
            name = eventHandler.getName();
            enabled = eventHandler.isRunning();
            directory = eventHandler.interpolatedDirectory().toString();
            pipelineName = eventHandler.getPipelineName();
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
            if (matcher.group(LABEL_GROUP_NUMBER) != null) {

                // Remember to strip off the trailing "." !
                labels.add(matcher.group(LABEL_GROUP_NUMBER)
                    .substring(0, matcher.group(LABEL_GROUP_NUMBER).length() - 1));
            } else if (count > 1) {
                throw new PipelineException(
                    "Events without ready file labels must have only 1 ready file");
            }
            filenames.add(matcher.group(0));
        }

        public boolean isReady() {
            return count == filenames.size();
        }

        @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
        public void delete(Path directory) {

            for (String filename : filenames) {
                try {
                    Files.delete(directory.resolve(filename));
                } catch (IOException e) {
                    throw new UncheckedIOException("Unable to delete file " + filename, e);
                }
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

        public Set<String> getLabels() {
            return labels;
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
            ReadyFile other = (ReadyFile) obj;
            return Objects.equals(name, other.name);
        }
    }
}

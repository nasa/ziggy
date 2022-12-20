package gov.nasa.ziggy.metrics.report;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.hibernate.service.config.spi.ConfigurationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.MatlabJavaInitialization;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * Manages the generation and capture of memory usage statistics. A configuration property is used
 * to indicate whether such statistics are to be generated and captured. If so, they are stored in
 * text files in the logs/memdrone directory under the pipeline results directory, with directories
 * for each pipeline module in each instance; the directories also have timestamps so that the most
 * recent directory can be identified.
 * <p>
 * Within a directory, memory statistics are stored in a text file, one per server / compute node.
 * The memory use of each process with the specified module name is determined using the ps command
 * at a user-specified interval. This is accomplished by running a shell script that performs the ps
 * command and appends it to the appropriate text file.
 *
 * @author PT
 */
public class Memdrone {
    private static final Logger log = LoggerFactory.getLogger(Memdrone.class);

    static final String MEMDRONE_STATS_CACHE_FILENAME = ".memdrone-stats-cache.ser";
    static final String PID_MAP_CACHE_FILENAME = ".pid-map-cache.ser";
    /**
     * Stores the relationship between the name root of a {@link Memdrone} instance and the
     * {@link ExecuteWatchdog} associated with its running shell script. Must be static so that an
     * instance of {@link Memdrone} other than the one that started the script is able to stop it.
     */
    private static Map<String, ExecuteWatchdog> watchdogMap = new HashMap<>();

    private final String binaryName;
    private final long instanceId;
    private final Path memdroneRootPath;
    private final String nameRoot;
    private Date date;

    /**
     * Uses the {@link ConfigurationService} to determine whether memory usage statistics should be
     * obtained.
     */
    public static final boolean memdroneEnabled() {
        return ZiggyConfiguration.getInstance()
            .getBoolean(PropertyNames.MEMDRONE_ENABLED_PROP_NAME, false);
    }

    public Memdrone(String binaryName, long instanceId) {
        this.binaryName = binaryName;
        this.instanceId = instanceId;
        memdroneRootPath = DirectoryProperties.memdroneDir();
        nameRoot = memdroneNameInvariantPart();
    }

    /**
     * Creates a new directory for the specified module name and instance ID. The directory is
     * created with the current time as its timestamp and is returned as a {@link Path} instance.
     *
     * @return the {@link Path} for the new directory.
     * @throws IOException if directory creation fails.
     */
    Path createNewMemdronePath() throws IOException {
        Path memdronePath = memdroneRootPath
            .resolve(nameRoot + Iso8601Formatter.dateTimeLocalFormatter().format(date()));
        Files.createDirectories(memdronePath);
        return memdronePath;
    }

    /**
     * Determines the latest directory for memory statistics by listing all the directories that
     * match the module name and instance ID, and returning the one with the most recent timestamp
     * in its name.
     *
     * @return {@link Path} for the most recent memory statistics directory.
     * @throws IOException if listing the files in the statistics main directory fails.
     */
    public Path latestMemdronePath() throws IOException {
        Path memdronePath = null;
        if (Files.exists(memdroneRootPath) && Files.isDirectory(memdroneRootPath)) {
            Pattern filenamePattern = Pattern.compile(nameRoot + "([0-9]{8})T([0-9]{6})");
            try (Stream<Path> stream = Files.list(memdroneRootPath)) {
                List<Path> matchingDirs = stream.filter(Files::isDirectory)
                    .filter(s -> filenamePattern.matcher(s.getFileName().toString()).matches())
                    .collect(Collectors.toList());
                if (matchingDirs != null && !matchingDirs.isEmpty()) {
                    Optional<Path> result = matchingDirs.stream()
                        .max((o1, o2) -> o1.getFileName()
                            .toString()
                            .compareTo(o2.getFileName().toString()));
                    memdronePath = result.get();
                }
            }
        }
        if (memdronePath == null) {
            memdronePath = createNewMemdronePath();
        }
        return memdronePath;
    }

    private String memdroneNameInvariantPart() {
        return binaryName + "-" + Long.toString(instanceId) + "-";
    }

    /**
     * Starts the shell script that acquires memory usage information. Before executing this, the
     * user must create the directory for storing memory usage statistics.
     *
     * @throws IOException if the path for the memory statistics cannot be located.
     */
    public void startMemdrone() throws IOException {

        Configuration config = ZiggyConfiguration.getInstance();
        CommandLine commandLine = new CommandLine(
            DirectoryProperties.ziggyBinDir().resolve("memdrone").toString());
        commandLine.addArgument(binaryName);
        commandLine.addArgument(config.getString(PropertyNames.MEMDRONE_SLEEP_PROP_NAME, "60"));
        Path memdronePath = latestMemdronePath();
        commandLine.addArgument(memdronePath.toString());
        if (watchdogMap.containsKey(nameRoot)) {
            log.info("Memdrone for module " + binaryName + ", instance " + instanceId
                + " already running");
            return;
        }

        log.info("Starting memdrone for module " + binaryName + " in instance " + instanceId);
        ExternalProcess memdroneProcess = ExternalProcess.simpleExternalProcess(commandLine);
        memdroneProcess.execute(false);
        watchdogMap.put(nameRoot, memdroneProcess.getWatchdog());
    }

    /**
     * Stops the shell script that collects memory usage information.
     */
    public void stopMemdrone() {
        if (watchdogMap.containsKey(nameRoot)) {
            log.info("Stopping memdrone for module " + binaryName + " in instance " + instanceId);
            watchdogMap.get(nameRoot).destroyProcess();
            log.info("Memdrone stopped");
            watchdogMap.remove(nameRoot);
        } else {
            log.info("No memdrone script was running for module " + binaryName + " in instance "
                + instanceId);
        }
    }

    /**
     * Collects the memory usage statistics for each process and returns as a {@link Map}. If the
     * information has already been collected and serialized, it is deserialized and returned.
     *
     * @return a {@link Map} from process ID to memory usage time series.
     * @throws Exception if any file operations fail.
     */
    public Map<String, DescriptiveStatistics> statsByPid() throws Exception {
        Path cacheFile = latestMemdronePath().resolve(MEMDRONE_STATS_CACHE_FILENAME);
        Map<String, DescriptiveStatistics> taskStats = null;

        if (Files.exists(cacheFile)) {
            log.info("Using stats cache file");
            try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(cacheFile.toFile())))) {
                @SuppressWarnings("unchecked")
                Map<String, DescriptiveStatistics> obj = (Map<String, DescriptiveStatistics>) ois
                    .readObject();
                taskStats = obj;
            }
        } else { // no cache
            log.info("Creating stats cache file");
            taskStats = createStatsCache();
        }

        return taskStats;
    }

    /**
     * Generates a {@link Map} between each process ID and the corresponding task/subtask. If the
     * mapping has already been generated and serialized, it is deserialized and returned.
     *
     * @return {@link Map} from process ID to task information.
     * @throws Exception if any file operations fail.
     */
    public Map<String, String> subTasksByPid() throws Exception {
        Path cacheFile = latestMemdronePath().resolve(PID_MAP_CACHE_FILENAME);
        Map<String, String> pidToSubTask = null;

        if (Files.exists(cacheFile)) {
            log.info("Using pid cache file");
            try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(cacheFile.toFile())))) {
                @SuppressWarnings("unchecked")
                Map<String, String> obj = (Map<String, String>) ois.readObject();
                pidToSubTask = obj;

                log.debug("pid cache: " + obj);
            }
        } else { // no cache
            log.info("Creating pid cache file");
            pidToSubTask = createPidMapCache();
        }

        return pidToSubTask;
    }

    /**
     * Generates and serializes the {@link Map} between process ID and memory usage time series.
     *
     * @throws Exception if any file operations fail.
     */
    public Map<String, DescriptiveStatistics> createStatsCache() throws Exception {
        Path latestMemdronePath = latestMemdronePath();
        Path cacheFile = latestMemdronePath.resolve(MEMDRONE_STATS_CACHE_FILENAME);
        Map<String, DescriptiveStatistics> taskStats = new HashMap<>();

        File[] memdroneLogs = latestMemdronePath.toFile()
            .listFiles((FileFilter) f -> f.getName().startsWith("memdrone-") && f.isFile());

        if (memdroneLogs != null) {
            log.info("Number of memdrone-* files found: " + memdroneLogs.length);

            for (File memdroneLog : memdroneLogs) {
                log.info("Processing: " + memdroneLog);

                String filename = memdroneLog.getName();
                String host = filename.substring(filename.indexOf("-") + 1, filename.indexOf("."));
                MemdroneLog mLog = new MemdroneLog(memdroneLog);
                Map<String, DescriptiveStatistics> contents = mLog.getLogContents();

                Set<String> pids = contents.keySet();

                for (String pid : pids) {
                    taskStats.put(host + ":" + pid, contents.get(pid));
                }
            }

            try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(cacheFile.toFile())))) {
                oos.writeObject(taskStats);
                oos.flush();
            } catch (Exception e) {
                log.warn("failed to create cache file, caught e = " + e);
            }
        }

        return taskStats;
    }

    /**
     * Generates and serializes the {@link} map between process ID and task/subtask.
     *
     * @throws Exception if any file operations fail.
     */
    public Map<String, String> createPidMapCache() throws Exception {
        Path cacheFile = latestMemdronePath().resolve(PID_MAP_CACHE_FILENAME);
        Map<String, String> pidToSubTask = new HashMap<>();

        log.info("cacheFile: " + cacheFile);

        Pattern taskPattern = Pattern
            .compile(binaryName + "-" + Long.toString(instanceId) + "-" + "\\d+");

        // Find the task directories and loop over them.
        File[] taskDirs = DirectoryProperties.taskDataDir()
            .toFile()
            .listFiles((FileFilter) f -> taskPattern.matcher(f.getName()).matches());
        if (taskDirs != null) {
            for (File taskDir : taskDirs) {
                log.info("Processing task " + taskDir.getName());
                Matcher taskMatcher = taskPattern.matcher(taskDir.getName());
                taskMatcher.matches();
                String taskDirName = taskDir.getName();
                taskDirName = taskDirName.substring(taskDirName.lastIndexOf("-") + 1);

                // Find the subtask directories and loop over them.
                File[] subTaskDirs = taskDir
                    .listFiles((FileFilter) f -> f.getName().contains("st-") && f.isDirectory());
                if (subTaskDirs != null) {
                    for (File subTaskDir : subTaskDirs) {
                        String subTaskId = taskDirName + "/" + subTaskDir.getName();
                        log.debug("processing subTaskId: " + subTaskId);

                        // Get the contents of the PID filename and populate the map
                        File pidsFile = new File(subTaskDir,
                            MatlabJavaInitialization.MATLAB_PIDS_FILENAME);
                        if (pidsFile.exists()) {
                            String hostPid = FileUtils.readFileToString(pidsFile,
                                MatlabJavaInitialization.PID_FILE_CHARSET);

                            pidToSubTask.put(hostPid, subTaskId);

                        }
                    }

                }
            }
        }

        try (

            ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(
                new FileOutputStream(cacheFile.toAbsolutePath().toString())))) {
            oos.writeObject(pidToSubTask);
            oos.flush();
        } catch (Exception e) {
            log.warn("failed to create cache file, caught e = " + e);
        }
        return pidToSubTask;
    }

    private Date date() {
        if (date == null) {
            return new Date();
        }
        return date;
    }

    // This method is for test purposes only
    void setDate(Date date) {
        this.date = date;
    }

}

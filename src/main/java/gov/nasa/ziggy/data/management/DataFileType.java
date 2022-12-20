package gov.nasa.ziggy.data.management;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.util.RegexBackslashManager;
import gov.nasa.ziggy.util.RegexGroupCounter;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * Defines a data file type. A data file type is a named object that has the following properties:
 * <ol>
 * <li>A name convention specification that specifies the name that a data file of this type has
 * when located in a task directory. This convention takes the form of a Java regex and is used to
 * match files in the task directory (i.e., a file is identified to be of a given type if its
 * filename matches the regex).
 * <li>A name convention specification that specifies the name and location that a data file of this
 * type has when located in the datastore. The location includes all directories below the datastore
 * root.
 * </ol>
 * <p>
 * The class uses regex group numbers to map groups from the task directory specification into the
 * datastore specification. In other words, a directory specification of "$1/$3/foo-$2/$4.h5" means
 * that the contents of group 1 is the name of the first directory below the datastore root, then
 * group 3, then "foo-" plus group 2, then group 4 plus ".h5".
 *
 * @author PT
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_DATA_FILE_TYPE")
public class DataFileType implements Persistable {

    public enum RegexType {
        TASK_DIR {
            @Override
            public Pattern getPattern(DataFileType dataFileType) {
                return dataFileType.fileNamePatternForTaskDir();
            }

            @Override
            public DataFilePaths dataFilePaths(Path datastoreRoot, Path taskDirectory,
                DataFileType dataFileType, Path dataFile) {
                Path sourcePath = datastoreRoot.resolve(dataFile);
                String destinationName = dataFileType
                    .taskDirFileNameFromDatastoreFileName(dataFile.toString());
                Path destinationPath = taskDirectory.resolve(destinationName);
                DataFilePaths paths = new DataFilePaths(sourcePath, destinationPath);
                paths.setDatastorePathToSource();
                return paths;
            }

            @Override
            public Stream<Path> pathStream(Path directory) throws IOException {
                return Files.list(directory);
            }

            @Override
            public Path pathToRelativize(Path directory, Path datastoreRoot) {
                return directory;
            }

        },
        DATASTORE {
            @Override
            public Pattern getPattern(DataFileType dataFileType) {
                return dataFileType.fileNamePatternForDatastore();
            }

            @Override
            public DataFilePaths dataFilePaths(Path datastoreRoot, Path taskDirectory,
                DataFileType dataFileType, Path dataFile) {
                Path sourcePath = taskDirectory.resolve(dataFile);
                String destinationName = dataFileType
                    .datastoreFileNameFromTaskDirFileName(dataFile.toString());
                Path destinationPath = datastoreRoot.resolve(destinationName);
                try {
                    Files.createDirectories(destinationPath.getParent());
                } catch (IOException e) {
                    throw new PipelineException(
                        "Unable to create directory " + destinationPath.getParent(), e);
                }
                DataFilePaths paths = new DataFilePaths(sourcePath, destinationPath);
                paths.setDatastorePathToDestination();
                return paths;
            }

            @Override
            public Stream<Path> pathStream(Path directory) throws IOException {
                return Files.walk(directory);
            }

            @Override
            public Path pathToRelativize(Path directory, Path datastoreRoot) {
                return datastoreRoot;
            }

        };

        public abstract Pattern getPattern(DataFileType dataFileType);

        public abstract DataFilePaths dataFilePaths(Path datastoreRoot, Path taskDirectory,
            DataFileType dataFileType, Path dataFile);

        public abstract Stream<Path> pathStream(Path directory) throws IOException;

        public abstract Path pathToRelativize(Path directory, Path datastoreRoot);

    }

    @Transient
    // Pattern that will match $ followed by a number, but will not match \$ plus a number
    private static final Pattern GROUP_NUMBER_PATTERN = Pattern.compile("(\\$[0-9]+)");

    @XmlAttribute(required = true)
    @Id
    private String name;

    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(RegexBackslashManager.XmlRegexAdapter.class)
    private String fileNameRegexForTaskDir;

    @XmlAttribute(required = true)
    private String fileNameWithSubstitutionsForDatastore;

    @ProxyIgnore
    @Transient
    private String fileNameRegexForDatastore;

    @ProxyIgnore
    @Transient
    private Pattern fileNamePatternForTaskDir;

    @ProxyIgnore
    @Transient
    private Pattern fileNamePatternForDatastore;

    @ProxyIgnore
    @Transient
    private Map<Integer, Integer> taskDirGroupToDatastoreGroupMap;

    // Used by Hibernate, do not remove.
    public DataFileType() {

    }

    /**
     * Checks that the groups in the task dir regex are consistent with the substitutions in the
     * datastore spec, that the datastore spec substitutions are valid, and that the name is not
     * blank.
     */
    public void validate() {

        // Check that the number of groups equals the number of substitutions, and
        // that every group maps to a substitution
        int nGroups = countFileNameGroups();
        int nSubs = countAndValidateSubstitutions();
        if (nGroups != nSubs) {
            throw new IllegalStateException("Number of task dir groups, " + nGroups
                + " does not equal number of datastore substitutions, " + nSubs);
        }

        // Check for a name
        if (name == null || name.isEmpty()) {
            throw new IllegalStateException("No name specified");
        }
    }

    /**
     * Counts the number of groups in the task dir file name regex.
     */
    private int countFileNameGroups() {
        return RegexGroupCounter.groupCount(fileNamePatternForTaskDir().pattern());
    }

    /**
     * Counts the number of substitution labels in the datastore file name specification. Also
     * validates that all substitutions from 1 to some max value are present.
     */
    private int countAndValidateSubstitutions() {
        Matcher groupNumberMatcher = GROUP_NUMBER_PATTERN
            .matcher(getFileNameWithSubstitutionsForDatastore());
        List<Integer> substitutionList = new ArrayList<>();
        while (groupNumberMatcher.find()) {
            String groupNumberString = groupNumberMatcher.group(1);
            int groupNumber = Integer.parseInt(groupNumberString.substring(1));
            substitutionList.add(groupNumber);
        }
        Set<Integer> substitutionSet = new HashSet<>(substitutionList);
        boolean subNumbersOkay = true;
        for (int i = 1; i <= substitutionSet.size(); i++) {
            if (!substitutionSet.contains(i)) {
                subNumbersOkay = false;
            }
        }
        if (!subNumbersOkay) {
            throw new IllegalStateException(
                "Datastore specification does not contain contiguous substitutions from 1 to "
                    + substitutionSet.size());
        }
        return substitutionSet.size();
    }

    public Pattern fileNamePatternForTaskDir() {
        if (fileNamePatternForTaskDir == null) {
            fileNamePatternForTaskDir = Pattern.compile(fileNameRegexForTaskDir);
        }
        return fileNamePatternForTaskDir;
    }

    /**
     * Generates a new version of the datastore name, with the group numbers replaced by the
     * equivalent group regex definitions from the taskDirName
     *
     * @return
     */
    public String fileNameRegexForDatastore() {
        if (fileNameRegexForDatastore == null) {

            List<Integer> groupsAlreadyUsed = new ArrayList<>();
            taskDirGroupToDatastoreGroupMap = new HashMap<>();
            // find all the groups in the taskDirName string
            Matcher groupMatcher = RegexGroupCounter.GROUP_PATTERN
                .matcher(fileNamePatternForTaskDir().pattern());
            List<String> taskDirGroups = new ArrayList<>();
            while (groupMatcher.find()) {
                taskDirGroups.add(groupMatcher.group(1));
            }

            // find and replace $(groupNumber) in datastoreName with the group contents
            Matcher groupNumberMatcher = GROUP_NUMBER_PATTERN
                .matcher(getFileNameWithSubstitutionsForDatastore());
            StringBuffer datastoreNameStringBuffer = new StringBuffer();
            int datastoreGroupCounter = 0;
            while (groupNumberMatcher.find()) {
                String groupNumberString = groupNumberMatcher.group(1);
                int groupNumber = Integer.parseInt(groupNumberString.substring(1));
                int index = groupsAlreadyUsed.indexOf(groupNumber);
                String replacement;
                if (index == -1) {
                    datastoreGroupCounter++;
                    taskDirGroupToDatastoreGroupMap.put(groupNumber, datastoreGroupCounter);
                    replacement = "(" + taskDirGroups.get(groupNumber - 1) + ")";
                    replacement = RegexBackslashManager.toDoubleBackslash(replacement);
                    groupsAlreadyUsed.add(groupNumber);
                } else {
                    replacement = "\\\\" + (index + 1);
                }
                groupNumberMatcher.appendReplacement(datastoreNameStringBuffer, replacement);
            }
            groupNumberMatcher.appendTail(datastoreNameStringBuffer);
            fileNameRegexForDatastore = datastoreNameStringBuffer.toString();
        }
        return fileNameRegexForDatastore;
    }

    public Pattern fileNamePatternForDatastore() {
        if (fileNamePatternForDatastore == null) {
            fileNamePatternForDatastore = Pattern.compile(fileNameRegexForDatastore());
        }
        return fileNamePatternForDatastore;
    }

    /**
     * Determines whether a given task directory name matches the task directory name pattern.
     */
    public boolean fileNameInTaskDirMatches(String fileNameInTaskDir) {
        return fileNamePatternForTaskDir().matcher(fileNameInTaskDir).matches();
    }

    public boolean fileNameInDatastoreMatches(String fileNameInDatastore) {
        return fileNamePatternForDatastore().matcher(fileNameInDatastore).matches();
    }

    /**
     * Converts a task directory name to the corresponding datastore name for a file. This involves
     * extracting the groups using the task dir file name pattern and then substituting them into
     * the datastore file name pattern.
     *
     * @return Converted task dir file name if the name matches the task dir convention, null
     * otherwise.
     */
    public String datastoreFileNameFromTaskDirFileName(String taskDirName) {
        String dName = null;
        Matcher taskDirMatcher = fileNamePatternForTaskDir().matcher(taskDirName);
        if (taskDirMatcher.matches()) {
            Matcher groupNumberMatcher = GROUP_NUMBER_PATTERN
                .matcher(getFileNameWithSubstitutionsForDatastore());
            StringBuffer datastoreNameStringBuffer = new StringBuffer();
            while (groupNumberMatcher.find()) {
                String groupNumberString = groupNumberMatcher.group(1);
                int groupNumber = Integer.parseInt(groupNumberString.substring(1));
                groupNumberMatcher.appendReplacement(datastoreNameStringBuffer,
                    taskDirMatcher.group(groupNumber));
            }
            groupNumberMatcher.appendTail(datastoreNameStringBuffer);
            dName = datastoreNameStringBuffer.toString();
        }
        return dName;
    }

    /**
     * Converts a datastore name to the corresponding task directory name for a file. This involves
     * extracting the groups from the datastore name pattern and substituting them into the task dir
     * name pattern.
     *
     * @return Converted datastore file name if the name matches the datastore name convention, null
     * otherwise.
     */
    public String taskDirFileNameFromDatastoreFileName(String datastoreDirName) {
        String tName = null;
        Matcher datastoreNameMatcher = fileNamePatternForDatastore().matcher(datastoreDirName);
        if (datastoreNameMatcher.matches()) {
            Matcher groupMatcher = RegexGroupCounter.GROUP_PATTERN.matcher(fileNameRegexForTaskDir);
            StringBuffer taskDirStringBuffer = new StringBuffer();
            int taskDirGroup = 0;
            while (groupMatcher.find()) {
                taskDirGroup++;
                int datastoreGroupNumber = taskDirGroupToDatastoreGroupMap.get(taskDirGroup);
                groupMatcher.appendReplacement(taskDirStringBuffer,
                    datastoreNameMatcher.group(datastoreGroupNumber));
            }
            groupMatcher.appendTail(taskDirStringBuffer);
            tName = taskDirStringBuffer.toString();
        }
        return tName;
    }

    /**
     * Returns a Pattern for a truncated version of the datastore file name. The truncationLevel
     * argument determines how many levels below the datastore root to include. For example, if
     * truncationLevel == 2, the 2 levels of the Pattern below the datastore root will be included.
     */
    public Pattern getDatastorePatternTruncatedToLevel(int truncationLevel) {
        String[] splitRegex = splitDatastoreRegex();
        if (truncationLevel < 1 || truncationLevel > splitRegex.length) {
            throw new IllegalArgumentException("Unable to truncate regex "
                + fileNameRegexForDatastore() + " to level " + truncationLevel);
        }

        StringBuilder truncatedRegexBuilder = new StringBuilder();
        for (int i = 0; i < truncationLevel; i++) {
            truncatedRegexBuilder.append(splitRegex[i]);
            if (i < truncationLevel - 1) {
                truncatedRegexBuilder.append("/");
            }
        }
        return Pattern.compile(truncatedRegexBuilder.toString());
    }

    /**
     * Returns a Pattern for the datastore file name in which the lowest directory levels have been
     * truncated. The levelsToTruncate argument determines how many levels will be removed.
     */
    public Pattern getDatastorePatternWithLowLevelsTruncated(int levelsToTruncate) {
        String[] splitRegex = splitDatastoreRegex();
        if (levelsToTruncate < 0 || levelsToTruncate > splitRegex.length) {
            throw new IllegalArgumentException("Unable to remove lowest " + levelsToTruncate
                + " from regex " + fileNameRegexForDatastore());
        }
        int truncationLevel = splitRegex.length - levelsToTruncate;
        return getDatastorePatternTruncatedToLevel(truncationLevel);
    }

    private String[] splitDatastoreRegex() {
        return fileNameRegexForDatastore().split("/");
    }

    // Getters and setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFileNameRegexForTaskDir() {
        return fileNameRegexForTaskDir;
    }

    public void setFileNameRegexForTaskDir(String fileNameRegexForTaskDir) {
        this.fileNameRegexForTaskDir = fileNameRegexForTaskDir;
    }

    public String getFileNameWithSubstitutionsForDatastore() {
        return fileNameWithSubstitutionsForDatastore;
    }

    public void setFileNameWithSubstitutionsForDatastore(
        String fileNameWithSubstitutionsForDatastore) {
        this.fileNameWithSubstitutionsForDatastore = fileNameWithSubstitutionsForDatastore;
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
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        DataFileType other = (DataFileType) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }

}

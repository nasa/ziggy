package gov.nasa.ziggy.data.datastore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Provides recursive transiting of the datastore directory tree (as defined by
 * {@link DatastoreNode} and {@link DatastoreRegexp} instances) to validate {@link DataFileType}
 * locations, determine the locations of all datastore directories required for a task, etc.
 *
 * @author PT
 */
public class DatastoreWalker {

    private static final Logger log = LoggerFactory.getLogger(DatastoreWalker.class);

    // The nodes in a location are always separated by a slash regardless of what the
    // OS uses for the directory separator character.
    private static final String NODE_SEPARATOR = "/";
    private static final String REGEXP_VALUE_SEPARATOR = "$";
    private static final String REGEXP_VALUE_SEPARATOR_AS_REGEXP = "\\" + REGEXP_VALUE_SEPARATOR;

    // NB This is how SpotBugs suggests to handle the file separator so that
    // MacOS, Linux, and Windows all work correctly.
    private static final String FILE_SEPARATOR = File.separatorChar == '\\' ? "\\\\"
        : File.separator;

    private Map<String, DatastoreRegexp> regexpsByName;
    private Map<String, DatastoreNode> datastoreNodesByFullPath;
    private Path datastoreRootPath = Paths.get(
        ZiggyConfiguration.getInstance().getString(PropertyName.DATASTORE_ROOT_DIR.property()));

    public DatastoreWalker(Map<String, DatastoreRegexp> regexpsByName,
        Map<String, DatastoreNode> datastoreNodesByFullPath) {
        this.regexpsByName = regexpsByName;
        this.datastoreNodesByFullPath = datastoreNodesByFullPath;
    }

    /**
     * Creates a {@link DatastoreWalker} objects from the {@link DatastoreRegexp}s and
     * {@link DatastoreNode}s in the database.
     *
     * @see DatastoreRegexpCrud#retrieveRegexpsByName()
     * @see DatastoreNodeCrud#retrieveNodesByFullPath()
     */
    public static DatastoreWalker newInstance() {
        return new DatastoreOperations().newDatastoreWalkerInstance();
    }

    /**
     * Validates a location from a {@link DataFileType} instance.
     * <p>
     * {@link #locationExists(String)} takes a location string of the form used in an instance of
     * {@link DataFileType} and performs the following validations on it:
     * <ol>
     * <li>The location exists.
     * <li>Any location element that includes a regexp component (i.e., .../cadenceType$ffi/...) has
     * only one such component.
     * <li>Any location element that includes a regexp component is a location that is a reference
     * to a {@link DatastoreRegexp} instance.
     * <li>Any location element that includes a regexp component has a valid regexp component.
     * </ol>
     */
    public boolean locationExists(String location) {

        String[] locationElements = location.split(NODE_SEPARATOR);
        List<LocationAndRegexpValue> locationsAndRegexpValues = new ArrayList<>();
        for (String locationElement : locationElements) {
            locationsAndRegexpValues.add(new LocationAndRegexpValue(locationElement));
        }

        // If any of the locations had more than one $ in it, that's an instant fail.
        List<String> invalidLocations = new ArrayList<>();
        for (int locationIndex = 0; locationIndex < locationsAndRegexpValues
            .size(); locationIndex++) {
            if (locationsAndRegexpValues.get(locationIndex).getLocation() == null) {
                invalidLocations.add(
                    locationElements[locationIndex].split(REGEXP_VALUE_SEPARATOR_AS_REGEXP)[0]);
            }
        }
        if (!invalidLocations.isEmpty()) {
            log.error("Location elements with too many $ characters: {}",
                invalidLocations.toString());
            return false;
        }

        // Construct the full path of the location
        String fullPath = fullPathFromLocations(locationsAndRegexpValues);

        // If the full path does not exist as a datastore node, that's a failure.
        if (!datastoreNodesByFullPath.containsKey(fullPath)) {
            log.error("Full path {} does not exist as a datastore node", fullPath);
            return false;
        }

        // Check that any regexp portions of any location elements are valid.
        invalidLocations = invalidRegexpLocations(locationsAndRegexpValues);
        if (!CollectionUtils.isEmpty(invalidLocations)) {
            log.error("Invalid regexp locations and/or definitions: {}",
                invalidLocations.toString());
            return false;
        }
        return true;
    }

    private List<String> invalidRegexpLocations(
        List<LocationAndRegexpValue> locationsAndRegexpValues) {
        List<String> invalidLocations = new ArrayList<>();
        StringBuilder incrementalPathBuilder = new StringBuilder();
        for (LocationAndRegexpValue locationAndRegexpValue : locationsAndRegexpValues) {
            if (incrementalPathBuilder.length() > 0) {
                incrementalPathBuilder.append(NODE_SEPARATOR);
            }
            String location = locationAndRegexpValue.getLocation();
            incrementalPathBuilder.append(location);
            String incrementalFullPath = incrementalPathBuilder.toString();
            if (!datastoreNodesByFullPath.containsKey(incrementalFullPath)) {
                invalidLocations.add(location);
                continue;
            }
            if (!datastoreNodesByFullPath.get(incrementalFullPath).isRegexp()) {
                continue;
            }
            DatastoreRegexp regexp = regexpsByName.get(locationAndRegexpValue.getLocation());
            if (regexp == null) {
                invalidLocations.add(location);
                continue;
            }
            String locationRegexp = locationAndRegexpValue.getRegexpValue();
            if (!regexp.matchesValue(locationRegexp) && !StringUtils.isBlank(locationRegexp)) {
                invalidLocations.add(location);
            }
        }
        return invalidLocations;
    }

    /**
     * Determines whether a given location represents a potentially valid directory in the
     * datastore.
     * <p>
     * The {@link #locationMatchesDatastore(String)} takes as its argument a definite location in
     * the datastore and determines whether, under the datastore layout as defined by the
     * {@link DatastoreNode}s, that location would be valid. This is accomplished by breaking the
     * location into its component elements; then, for each element, looking to see whether that
     * element matches any nodes (an exact match is required for nodes that are not pointers to
     * {@link DatastoreRegexp}s; for nodes that point to DatastoreRegexp instances, the directory
     * location has to match the {@link Pattern} derived from the regexp's value field). If there is
     * a match, the set of nodes that are tested against the next element are the child nodes of the
     * matching node. If every element of the location matches a datastore node,
     * {@link #locationMatchesDatastore(String)} returns true, otherwise false.
     * <p>
     * Consider an example in which the top node of the datastore is a {@link DatastoreRegexp}, with
     * name "sector" and value "sector-[0-9]{4}" (i.e, "sector" followed by a hyphen followed by a 4
     * digit number). The sector node has two child nodes, "mda" and "1sa", each of which is a
     * non-regexp {@link DatastoreNode}. The following location arguments will return true:
     *
     * <pre>
     * sector-0002/mda
     * sector-1024/1sa
     * </pre>
     *
     * The following location arguments will return false:
     *
     * <pre>
     * sector-1/mda        (sector regexp is not matched.)
     * mda                 (there is no mda datastore node at the top of the tree.)
     * sector-0002/tbr     (there is no tbr node under the sector node.)
     * sector-0002/mda/cal (there is no cal node under sector/mda.)
     * </pre>
     *
     * The {@link #locationMatchesDatastore(String)} allows a user to determine whether a specific
     * directory would violate the datastore layout, and thus allows the user to ensure that no
     * datastore directories are ever created that violate that layout (any such directory would be
     * unreachable by the datastore API).
     */
    public boolean locationMatchesDatastore(String location) {
        String[] locationElements = location.split(NODE_SEPARATOR);
        List<DatastoreNode> datastoreNodes = new ArrayList<>(datastoreNodesByFullPath.values());
        for (String locationElement : locationElements) {

            // If there are still location elements but no further nodes down this
            // part of the tree, then the location is not a potentially valid one.
            if (datastoreNodes.isEmpty()) {
                return false;
            }
            DatastoreNode matchingNode = null;

            // Find a current node which matches this element of the location.
            for (DatastoreNode node : datastoreNodes) {
                if (node.isRegexp()) {
                    DatastoreRegexp regexp = regexpsByName.get(node.getName());
                    if (regexp.matchesValue(locationElement)) {
                        matchingNode = node;
                        continue;
                    }
                } else if (node.getName().equals(locationElement)) {
                    matchingNode = node;
                }
            }

            // If we didn't find a match, this is not a valid potential location.
            if (matchingNode == null) {
                return false;
            }

            // Put the child nodes of the current node into the datastoreNodes collection.
            datastoreNodes.clear();
            if (!CollectionUtils.isEmpty(matchingNode.getChildNodeFullPaths())) {
                for (String fullPath : matchingNode.getChildNodeFullPaths()) {
                    datastoreNodes.add(datastoreNodesByFullPath.get(fullPath));
                }
            }
        }
        return true;
    }

    String fullPathFromLocations(List<LocationAndRegexpValue> locationsAndRegexpValues) {
        StringBuilder sb = new StringBuilder();
        for (LocationAndRegexpValue locationAndRegexpValue : locationsAndRegexpValues) {
            sb.append(locationAndRegexpValue.getLocation());
            sb.append(NODE_SEPARATOR);
        }
        sb.setLength(sb.length() - NODE_SEPARATOR.length());
        return sb.toString();
    }

    /**
     * Returns all the existing datastore directories that exist and that match a given datastore
     * location.
     * <p>
     * The {@link #pathsForLocation(String)} takes a datastore path and walks the directories below
     * the datastore root directory. It locates all the directories that match the datastore
     * location argument and returns them in a list.
     */
    public List<Path> pathsForLocation(String location) {
        if (!locationExists(location)) {
            throw new IllegalArgumentException("Datastore location " + location + " not valid");
        }

        String[] locationElements = location.split(NODE_SEPARATOR);
        List<LocationAndRegexpValue> locationsAndRegexpValues = new ArrayList<>();
        for (String locationElement : locationElements) {
            locationsAndRegexpValues.add(new LocationAndRegexpValue(locationElement));
        }
        return pathsForLocation(locationsAndRegexpValues, 0, datastoreRootPath);
    }

    /**
     * Performs the iterative portion of searching for datastore directory paths. The iteration is
     * over subdirectory levels. At each step, a check is performed to see if there are additional
     * subdirectories below the current directory level. If so,
     * {@link #pathsForLocation(List, int, Path)} is called for each of the subdirectories below the
     * current level. Otherwise, the method returns, causing all the calls to
     * {@link #pathsForLocation(List, int, Path)} to return.
     */
    private List<Path> pathsForLocation(List<LocationAndRegexpValue> locationsAndRegexpValues,
        int locationIndex, Path parentPath) {

        // Where are we so far?
        String fullPathFromLocations = fullPathFromLocations(
            locationsAndRegexpValues.subList(0, locationIndex + 1));

        DatastoreNode node = datastoreNodesByFullPath.get(fullPathFromLocations);
        List<Path> pathsThisLevel = List.of(parentPath.resolve(node.getName()));

        // If this is a regexp, find all the directories at this level based on the
        // regexp value in the location (if any) and the include / exclude regexps
        if (node.isRegexp()) {
            DatastoreRegexp regexp = regexpsByName.get(node.getName());
            pathsThisLevel = listPaths(parentPath,
                locationsAndRegexpValues.get(locationIndex).getRegexpValue(), regexp);
        }

        // If we've gone as far down this location as possible, we can return the paths
        // found at this level.
        if (locationIndex == locationsAndRegexpValues.size() - 1) {
            return pathsThisLevel;
        }

        // Otherwise, go down another level
        List<Path> pathsNextLevel = new ArrayList<>();
        for (Path path : pathsThisLevel) {
            pathsNextLevel
                .addAll(pathsForLocation(locationsAndRegexpValues, locationIndex + 1, path));
        }
        return pathsNextLevel;
    }

    /**
     * Finds the paths of subdirectories of the current parent path that match a given
     * {@link DatastoreRegexp}. This takes into account the value, include, and exclude fields of
     * the DatastoreRegexp.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private List<Path> listPaths(Path parentPath, String regexpValueThisLevel,
        DatastoreRegexp regexp) {
        List<Path> pathsThisLevel = new ArrayList<>();
        if (!Files.exists(parentPath) || !Files.isDirectory(parentPath)) {
            return pathsThisLevel;
        }
        DirectoryStream.Filter<? super Path> dirFilter = path -> regexp
            .matches(path.getFileName().toString())
            && (StringUtils.isBlank(regexpValueThisLevel)
                || path.getFileName().toString().matches(regexpValueThisLevel));
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(parentPath, dirFilter)) {
            for (Path entry : dirStream) {
                pathsThisLevel.add(entry.toAbsolutePath());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return pathsThisLevel;
    }

    /**
     * Returns the indices of {@link Path} name elements that vary across a collection of path
     * instances. These are used to construct brief states that contain only name elements that
     * change from one UOW to the next (i.e., if the paths are "foo/bar/baz" and "foo/baz/bar", we
     * only need the "baz" and "bar" elements in the brief state; the "foo" is common to all units
     * of work so it doesn't tell us anything).
     */
    public List<Integer> pathElementIndicesForBriefState(List<Path> datastorePaths) {

        // Handle the special case of only one Path in the list.
        List<Integer> pathElementIndicesForBriefState = new ArrayList<>();
        if (datastorePaths.size() == 1) {
            return pathElementIndicesForBriefState;
        }

        // Determine which elements vary across the collection of datastore paths. A set in the
        // "elements" list will only have one item if the element is common across all paths.
        int nameCount = datastorePaths.get(0).getNameCount();
        List<Set<Path>> elements = new ArrayList<>();
        for (int i = 0; i < nameCount; i++) {
            elements.add(new HashSet<>());
        }
        for (Path path : datastorePaths) {
            if (nameCount != path.getNameCount()) {
                throw new IllegalArgumentException(path.toString() + " has " + path.getNameCount()
                    + " elements, but " + nameCount + " was expected");
            }
            for (int elementIndex = 0; elementIndex < path.getNameCount(); elementIndex++) {
                elements.get(elementIndex).add(path.getName(elementIndex).getFileName());
            }
        }

        // Find the indices of the path elements that have > 1 item (i.e., the ones that vary).
        for (int elementIndex = 0; elementIndex < nameCount; elementIndex++) {
            if (elements.get(elementIndex).size() > 1) {
                pathElementIndicesForBriefState.add(elementIndex);
            }
        }
        return pathElementIndicesForBriefState;
    }

    /**
     * Constructs a {@link Map} of regexp values by regexp name. This requires a location string
     * (which can be used to determine which elements of the path correspond to datastore nodes that
     * are regular expressions and which are single-valued nodes), and a path (so that the elements
     * of the path that are now known to be regexps can be captured and used to populate the Map).
     */
    public Map<String, String> regexpValues(String location, Path path) {
        return regexpValuesByRegexpName(location, path, true);
    }

    /**
     * Constructs a {@link Map} of regexp values by regexp name. This requires a location string
     * (which can be used to determine which elements of the path are regular expressions and which
     * are single-valued nodes), and a path (so that the elements of the path that are now known to
     * be regexps can be captured and used to populate the Map). The caller has the option to
     * suppress regexp values that are specified in the location string (i.e., "foo$bar"), or
     * populate same.
     */
    public Map<String, String> regexpValuesByRegexpName(String location, Path path,
        boolean includeValuesFromLocation) {
        Map<String, String> regexpValues = new LinkedHashMap<>();
        Map<String, String> regexpValuesInLocation = new LinkedHashMap<>();
        String[] pathElements = null;
        if (path != null) {
            if (path.isAbsolute()) {
                path = datastoreRootPath.toAbsolutePath().relativize(path);
            }
            pathElements = path.toString().split(FILE_SEPARATOR);
        }

        String[] locationElements = location.split(NODE_SEPARATOR);
        String cumulativePath = "";
        for (int i = 0; i < locationElements.length; i++) {
            if (cumulativePath.length() > 0) {
                cumulativePath = cumulativePath + NODE_SEPARATOR;
            }
            String[] locationSubelements = locationElements[i]
                .split(REGEXP_VALUE_SEPARATOR_AS_REGEXP);
            String truncatedLocation = locationSubelements[0];
            cumulativePath = cumulativePath + truncatedLocation;
            if (datastoreNodesByFullPath.get(cumulativePath).isRegexp()) {
                if (locationSubelements.length == 2 && !includeValuesFromLocation) {
                    continue;
                }
                if (pathElements != null) {
                    regexpValues.put(truncatedLocation, pathElements[i]);
                }
                if (locationSubelements.length == 2 && includeValuesFromLocation) {
                    regexpValuesInLocation.put(truncatedLocation, locationSubelements[1]);
                }
            }
        }
        return path != null ? regexpValues : regexpValuesInLocation;
    }

    /**
     * Constructs a {@link Path} for a location in which the regular expressions have been replaced
     * by specific values.
     */
    public Path pathFromLocationAndRegexpValues(Map<String, String> regexpValues, String location) {
        Path path = datastoreRootPath.toAbsolutePath();
        String[] locationElements = location.split(NODE_SEPARATOR);
        for (String locationElement : locationElements) {
            String[] locationAndValue = locationElement.split(REGEXP_VALUE_SEPARATOR_AS_REGEXP);
            String trimmedLocation = locationAndValue[0];
            if (regexpValues.get(trimmedLocation) != null) {

                // NB: if the location has an associated value
                // for the current element (i.e., "foo$bar"),
                // and there is also a regexp value for that element
                // (i.e., regexpValues.get("foo") == "baz"), the value
                // from the location takes precedence.
                String regexpValue = locationAndValue.length == 1
                    ? regexpValues.get(trimmedLocation)
                    : locationAndValue[1];
                path = path.resolve(regexpValue);
            } else {
                path = locationAndValue.length == 1 ? path.resolve(trimmedLocation)
                    : path.resolve(locationAndValue[1]);
            }
        }
        return path;
    }

    /**
     * Converts a {@link Path} to a specific location.
     * <p>
     * A path is a file path on the file system, with no regular expression argle-bargle. A
     * {@link DataFileType} contains a full location, which can include regular expressions and can
     * have more elements than the path does. The specific location is one in which the full
     * location and the path are merged to produce a String that the {@link DatastoreWalker} can use
     * to locate directories. Imagine for example that the path is "foo/baz", and the full location
     * is "foo/bar/duran/sisters\$mercy/bauhaus", where bar is a datastore regular expression. The
     * String returned by {@link #specificLocation(DataFileType, Path)} would be
     * "foo/bar$baz/duran/sisters$mercy/bauhaus".
     * <p>
     * The specific location does not necessarily correspond to a single file path on the file
     * system. To understand this, consider again the example in which the path is "foo/baz", but in
     * this case the full location is "foo/bar/duran/sisters/bauhaus", where "sisters" is a
     * reference to a datastore regular expression that can have values "mercy", "indulgence", or
     * "colleges". The specific location that combines these would be
     * "foo/bar$baz/duran/sisters/bauhaus". Note that the "bar" regular expression element is
     * expanded to "bar$baz", but "sisters" is not expanded. When given this specific location, the
     * {@link DatastoreWalker} will, if asked to find the file system paths that correspond to the
     * location, return "foo/baz/duran/mercy/bauhaus", "foo/baz/duran/indulgence/bauhaus", and
     * "foo/baz/duran/colleges/bauhaus".
     */
    public String specificLocation(DataFileType dataFileType, Path directory) {

        String[] fullLocationElements = elements(fullLocation(dataFileType));

        String[] datastoreDirectoryNameElements = datastorePathElements(directory);

        // Construct the specific location from its constituent elements.
        StringBuilder specificLocation = new StringBuilder();
        for (int locationIndex = 0; locationIndex < fullLocationElements.length; locationIndex++) {
            specificLocation.append(specificLocationElement(fullLocationElements,
                datastoreDirectoryNameElements, locationIndex));
            specificLocation.append(NODE_SEPARATOR);
        }
        specificLocation.setLength(specificLocation.length() - 1);
        return specificLocation.toString();
    }

    static String[] elements(String location) {
        return location.split(NODE_SEPARATOR);
    }

    /**
     * Break a {@link Path} into its specific path elements.
     * <p>
     * Given the Path "foo/baz/duran/sisters", this method will return a String array containing
     * "foo", "baz", "duran", "sisters". The path argument is first relativized to the datastore
     * root to ensure that its name elements correspond to the full location elements (i.e., in both
     * cases the first string in the array is "foo"). The result is returned as a String array to
     * match the data type of the full location elements.
     */
    private String[] datastorePathElements(Path directory) {
        Path datastoreDirectory = DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .relativize(directory.toAbsolutePath());
        String[] datastorePathElements = new String[datastoreDirectory.getNameCount()];
        int elementIndex = 0;
        for (Path pathElement : datastoreDirectory) {
            datastorePathElements[elementIndex++] = pathElement.toString();
        }
        return datastorePathElements;
    }

    /**
     * Generate the specific location element for a given element of the location. In most cases,
     * the specific location element is just the element from the full location. The exceptions are
     * as follows:
     * <ol>
     * <li>If the full location element contains "\$", it must be replaced with "$".
     * <li>If the full location element is a reference to a {@link DatastoreRegexp}, it must be
     * combined with the corresponding path name element: in other words, a location element of
     * "foo" and a path element of "bar" will result in a specific location element of "foo$bar".
     * </ol>
     */
    private String specificLocationElement(String[] fullLocationElements,
        String[] directoryNameElements, int fullLocationIndex) {

        // If there's a "\$" in the specified full location element, replace it with "$"
        String correctedFullLocationElement = fullLocationElements[fullLocationIndex]
            .replace(REGEXP_VALUE_SEPARATOR_AS_REGEXP, REGEXP_VALUE_SEPARATOR);
        // If we're past the end of the directory name elements, we return the location
        // element verbatim.
        if (fullLocationIndex >= directoryNameElements.length) {
            return correctedFullLocationElement;
        }

        String directoryNameElement = directoryNameElements[fullLocationIndex];

        // If the full location element contains a constraint that matches the path name element
        // (i.e., if the full location element is "foo$bar" and the path element is "bar"),
        // we can return the location element verbatim.
        if (correctedFullLocationElement.equals(directoryNameElement)
            || fullLocationConsistentWithDirectoryElement(correctedFullLocationElement,
                directoryNameElement)) {
            return correctedFullLocationElement;
        }

        // If the full location element contains a constraint that conflicts with the path name
        // element (i.e., the full location element is "foo$bar" and the path element is "baz"),
        // throw an exception.
        if (correctedFullLocationElement.contains(DatastoreWalker.REGEXP_VALUE_SEPARATOR)) {
            throw new PipelineException("Path name element " + directoryNameElement
                + " cannot be combined with location element " + correctedFullLocationElement);
        }
        return correctedFullLocationElement + DatastoreWalker.REGEXP_VALUE_SEPARATOR
            + directoryNameElement;
    }

    /**
     * Determines whether the full location element is consistent with the directory name element.
     * In this case, "consistent" means any of the following:
     * <ol>
     * <li>The full location element matches the directory name element.
     * <li>The full location element has a regexp constraint that matches the value of the directory
     * name element (i.e., "foo$bar" and "bar");
     * </ol>
     */
    private boolean fullLocationConsistentWithDirectoryElement(String correctedFullLocationElement,
        String directoryNameElement) {
        if (correctedFullLocationElement.equals(directoryNameElement)) {
            return true;
        }
        String[] correctedLocationSplitAtDollarSign = correctedFullLocationElement
            .split(REGEXP_VALUE_SEPARATOR_AS_REGEXP);
        if (correctedLocationSplitAtDollarSign.length == 1) {
            return false;
        }
        String regexpConstraint = correctedLocationSplitAtDollarSign[1];
        if (regexpConstraint.equals(directoryNameElement)) {
            return true;
        }
        return false;
    }

    /**
     * Determines the indices for location elements that must be searched for data files.
     * <p>
     * Consider a situation in which the location of a data file type is "foo/bar", but the full
     * location is "foo/bar/baz$duran/sisters/bauhaus". Any search for data files must start in
     * "foo/bar/duran" and include any directories that are referenced by the "sisters" and
     * "bauhaus" elements (i.e., if "bauhaus" is a DatastoreRegexp, then all the directories under
     * "foo/bar/duran/sisters" have to be searched). The
     * {@link #findLocationIndicesForSublocation(DataFileType)} method provides this information by
     * finding the indices for elements in the full location that (a) are after the end of the
     * location, and (b) do not contain a "$". For this example, the indices that match these
     * conditions are 3 and 4 (i.e., "sisters" and "bauhaus").
     * <p>
     * The indices are stored with an offset relative to the length of the location, i.e., they are
     * indices into the element of the full location that comes after the location. Thus, since the
     * location has 2 elements, the indices that would be returned in this example are 1 and 2.
     */
    public Set<Integer> findLocationIndicesForSublocation(DataFileType dataFileType) {

        // Use a TreeSet so that the element indices are in order.
        Set<Integer> locationIndicesForSublocation = new TreeSet<>();
        int locationElementsCount = elements(dataFileType.getLocation()).length;
        String[] fullLocationElements = elements(fullLocation(dataFileType));

        // Are there any components of the full location after the location?
        int numberOfElementsNotInLocation = fullLocationElements.length - locationElementsCount;
        if (numberOfElementsNotInLocation == 0) {
            return locationIndicesForSublocation;
        }

        // Loop over the elements that fall after the location. Reject any that contain
        // a "$". Capture the indices of the rest.
        for (int fullLocationElementsIndex = locationElementsCount; fullLocationElementsIndex < fullLocationElements.length; fullLocationElementsIndex++) {
            if (fullLocationElements[fullLocationElementsIndex].contains(REGEXP_VALUE_SEPARATOR)) {
                continue;
            }
            locationIndicesForSublocation.add(fullLocationElementsIndex - locationElementsCount);
        }
        return locationIndicesForSublocation;
    }

    public static String fileNameRegexpBaseName(DataFileType dataFileType) {
        String[] fileNameRegexpElements = elements(dataFileType.getFileNameRegexp());
        return fileNameRegexpElements[fileNameRegexpElements.length - 1];
    }

    public static String fullLocation(DataFileType dataFileType) {
        String[] fileNameRegexpElements = elements(dataFileType.getFileNameRegexp());
        String regexpLocation = Arrays
            .asList(Arrays.copyOf(fileNameRegexpElements, fileNameRegexpElements.length - 1))
            .stream()
            .collect(Collectors.joining(NODE_SEPARATOR));
        return StringUtils.isBlank(regexpLocation) ? dataFileType.getLocation()
            : dataFileType.getLocation() + NODE_SEPARATOR + regexpLocation;
    }

    /**
     * Full path string for a {@link DatastoreNode} when the full path of its parent is taken into
     * account. Package scoped for test purposes.
     */
    static String fullPathFromParentPath(String nodeName, String parentFullPath) {
        return StringUtils.isBlank(parentFullPath) ? nodeName
            : parentFullPath + NODE_SEPARATOR + nodeName;
    }

    Map<String, DatastoreRegexp> regexpsByName() {
        return regexpsByName;
    }

    Map<String, DatastoreNode> datastoreNodesByFullPath() {
        return datastoreNodesByFullPath;
    }

    private static class LocationAndRegexpValue {

        private final String location;
        private final String regexpValue;

        public LocationAndRegexpValue(String locationWithOptionalRegexp) {
            String[] locationComponents = locationWithOptionalRegexp.split("\\$");
            if (locationComponents.length > 2) {
                location = null;
                regexpValue = null;
                return;
            }
            location = locationComponents[0];
            regexpValue = locationComponents.length == 2 ? locationComponents[1] : "";
        }

        public String getLocation() {
            return location;
        }

        public String getRegexpValue() {
            return regexpValue;
        }
    }
}

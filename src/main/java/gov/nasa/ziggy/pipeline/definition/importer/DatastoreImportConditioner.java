package gov.nasa.ziggy.pipeline.definition.importer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Prepares the datastore imports for persisting to the database.
 * <p>
 * The datastore configuration consists of the following:
 * <ol>
 * <li>Definition of {@link DatastoreRegexp} instances.
 * <li>Definition of {@link DatastoreNode} instances.
 * <li>Definition of {@link DataFileType} instances.
 * <li>Definition of {@link ModelType} instances.
 * </ol>
 * <p>
 * Conditioning includes the following steps:
 * <ol>
 * <li>Check imports for duplicates.
 * <li>Apply updates from imported data objects to objects already in the database.
 * <li>Identify any {@link DatastoreNode}s that have been made obsolete by the imports.
 * </ol>
 *
 * @author PT
 */
public class DatastoreImportConditioner {

    private static final Logger log = LoggerFactory.getLogger(DatastoreImportConditioner.class);

    private List<DataFileType> importedDataFileTypes;
    private List<ModelType> importedModelTypes;
    private List<DatastoreRegexp> importedDatastoreRegexps;
    private List<DatastoreNode> importedDatastoreNodes;
    private List<String> fullPathsForNodesToRemove = new ArrayList<>();
    private Set<DatastoreNode> nodesForDatabase = new HashSet<>();

    private Map<String, DatastoreNode> databaseDatastoreNodesByFullPath;

    private DatastoreOperations datastoreOperations = new DatastoreOperations();
    private DatastoreWalker datastoreWalker;

    public DatastoreImportConditioner(List<DataFileType> importedDataFileTypes,
        List<ModelType> importedModelTypes, List<DatastoreRegexp> importedDatastoreRegexps,
        List<DatastoreNode> importedDatastoreNodes) {
        this.importedDataFileTypes = importedDataFileTypes;
        this.importedModelTypes = importedModelTypes;
        this.importedDatastoreRegexps = importedDatastoreRegexps;
        this.importedDatastoreNodes = importedDatastoreNodes;
    }

    /** Validates all imports. */
    public void checkDefinitions() {

        // Get all the datastore nodes out of the database.
        databaseDatastoreNodesByFullPath = datastoreOperations().datastoreNodesByFullPath();

        updateRegexps();
        checkAndUpdateNodeDefinitions();
        checkDataFileTypeDefinitions();
        checkModelTypeDefinitions();
    }

    /**
     * Updates {@link DatastoreRegexp} definitions that are present in the import but which are also
     * present in the database. Note that DatastoreRegexp definitions are never deleted. The
     * instance variable datastoreRegexps is updated to contain the new DatastoreRegexp definitions
     * and the updated DatastoreRegexp definitions. In the latter case, the actual objects in the
     * datastoreRegexps list are the objects retrieved from the database, since we need to modify
     * the database object in order to safely use the merge() method.
     * <p>
     * The method returns a List of DatastoreRegexp names that includes the new names, the names of
     * existing instances that are updated, and the names of existing instances that are not being
     * touched (i.e., it's the list of names that will be in the database after the merge).
     */
    private void updateRegexps() {
        List<DatastoreRegexp> regexpsToPersist = new ArrayList<>();

        // If there are no regexps in the import, we can skip this whole step.
        if (CollectionUtils.isEmpty(importedDatastoreRegexps)) {
            importedDatastoreRegexps = regexpsToPersist;
            return;
        }

        // Get the regexps out of the database.
        Map<String, DatastoreRegexp> databaseRegexpsByName = datastoreOperations()
            .datastoreRegexpsByName();
        Set<String> regexpNames = new HashSet<>(databaseRegexpsByName.keySet());
        for (DatastoreRegexp regexp : importedDatastoreRegexps) {

            // If the regexp is new, we need to persist it; if it matches one in the database,
            // we need to update the value of the database copy.
            DatastoreRegexp regexpToPersist = databaseRegexpsByName.containsKey(regexp.getName())
                ? databaseRegexpsByName.get(regexp.getName())
                : regexp;
            regexpToPersist.setValue(regexp.getValue());
            regexpsToPersist.add(regexpToPersist);
            regexpNames.add(regexpToPersist.getName());
            if (databaseRegexpsByName.containsKey(regexp.getName())) {
                log.warn(
                    "Datastore regexp {} already exists, updating database value from {} to {}",
                    regexp.getName(), databaseRegexpsByName.get(regexp.getName()).getValue(),
                    regexp.getValue());
            }
        }
        importedDatastoreRegexps = regexpsToPersist;
    }

    /**
     * Ensure that datastore nodes are defined correctly. Specifically, use the XML-only fields to
     * populate the database fields, and generate database-appropriate parent-child relationships
     * between the nodes.
     * <p>
     * The imported node definitions are merged with existing node definitions in the database. This
     * means that the identities of the child nodes are updated and the value of isRegexp is
     * updated. This process can result in DatastoreNode instances or even entire branches of the
     * node becoming obsolete. These nodes / branches will be deleted from the database table that
     * holds the DatastoreNode definitions.
     */
    private void checkAndUpdateNodeDefinitions() {

        Set<String> allRegexpNames = allRegexpNames();

        // Populate the set of DatabaseNode instances that will be persisted to the
        // database.
        findNodesForDatabase(allRegexpNames);

        for (DatastoreNode nodeForDatabase : nodesForDatabase) {
            if (!isNodeSelfConsistent(nodeForDatabase, allRegexpNames, false)) {
                log.warn("Unable to store datastore nodes in database due to validation failures");
                importedDatastoreNodes.clear();
                fullPathsForNodesToRemove.clear();
                return;
            }
        }
        log.info("All datastore nodes successfully populated");
    }

    private Set<String> allRegexpNames() {
        Set<String> allRegexpNames = importedDatastoreRegexps.stream()
            .map(DatastoreRegexp::getName)
            .collect(Collectors.toSet());
        allRegexpNames.addAll(datastoreOperations().regexpNames());

        return allRegexpNames;
    }

    /** Top-level method for locating the nodes that will be persisted. */
    private void findNodesForDatabase(Set<String> allRegexpNames) {
        if (CollectionUtils.isEmpty(importedDatastoreNodes)) {
            return;
        }
        findNodesForDatabase(importedDatastoreNodes, allRegexpNames, "");
    }

    /**
     * Business logic for locating nodes that will be persisted.
     * <p>
     * The method first checks to see whether a given node is a new node or is an update to an
     * existing node; if the latter, the existing node is updated with content from the imported
     * node. Several consistency checks are performed on the node contents. The child nodes to the
     * current node are located (either from imported or from existing database nodes), and the
     * {@link #findNodesForDatabase(List, Set, String)} method is called on the child nodes.
     * <p>
     */
    private void findNodesForDatabase(List<DatastoreNode> datastoreNodes,
        Set<String> allRegexpNames, String parentNodeFullPath) {
        for (DatastoreNode node : datastoreNodes) {

            node.setFullPath(fullPathFromParentPath(node, parentNodeFullPath));
            DatastoreNode nodeForDatabase = selectImportedOrDatabaseNode(node);

            // If we've already encountered a problem then we can't perform the
            // child node portion of the search because the child nodes may
            // contain duplicates.
            if (!isNodeSelfConsistent(nodeForDatabase, allRegexpNames, true)) {
                continue;
            }

            // Find the child nodes to the current nodes, some of which
            // may be database nodes.
            List<DatastoreNode> childNodes = childNodes(nodeForDatabase);
            nodesForDatabase.add(nodeForDatabase);
            // Generate nodeForDatabase instances for the child nodes.
            findNodesForDatabase(childNodes, allRegexpNames, node.getFullPath());
        }
    }

    /**
     * Full path string for a {@link DatastoreNode} when the full path of its parent is taken into
     * account.
     */
    private static String fullPathFromParentPath(DatastoreNode node, String parentFullPath) {
        return DatastoreWalker.fullPathFromParentPath(node.getName(), parentFullPath);
    }

    /**
     * Returns either the {@link DatastoreNode} passed as an argument, or else the existing database
     * node with the same full path. In the latter case, content from the imported node is copied to
     * the database node.
     */
    private DatastoreNode selectImportedOrDatabaseNode(DatastoreNode node) {

        DatastoreNode nodeForDatabase = databaseDatastoreNodesByFullPath.containsKey(
            node.getFullPath()) ? databaseDatastoreNodesByFullPath.get(node.getFullPath())
                : new DatastoreNode(node.getName(), node.isRegexp());
        nodeForDatabase.setRegexp(node.isRegexp());
        nodeForDatabase.setNodes(node.getNodes());
        nodeForDatabase.setXmlNodes(node.getXmlNodes());
        nodeForDatabase.setNodes(node.getNodes());
        nodeForDatabase.setFullPath(node.getFullPath());

        return nodeForDatabase;
    }

    /**
     * Performs self-consistency checks on a {@link DatastoreNode} and optionally generates log
     * messages in the event of any failures.
     */
    private boolean isNodeSelfConsistent(DatastoreNode node, Set<String> allRegexpNames,
        boolean doLogging) {

        // Check for undefined regexp.
        if (node.isRegexp() && !allRegexpNames.contains(node.getName())) {
            logOptionalErrorMessage(doLogging, "Node {} is undefined regexp", node.getName());
            return false;
        }

        // Check for duplicate child node names.
        List<String> childNodeNames = childNodeNames(node.getNodes());
        List<String> duplicateChildNodeNames = ZiggyStringUtils.duplicateStrings(childNodeNames);
        if (!CollectionUtils.isEmpty(duplicateChildNodeNames)) {
            logOptionalErrorMessage(doLogging, "Node {} has duplicate child names: {}",
                node.getFullPath(), duplicateChildNodeNames.toString());
            return false;
        }

        // Check XML nodes for duplicates
        List<String> duplicateXmlNodeNames = duplicateXmlNodeNames(node.getXmlNodes());
        if (!CollectionUtils.isEmpty(duplicateXmlNodeNames)) {
            logOptionalErrorMessage(doLogging, "Node {} has duplicate XML node names: {}",
                node.getFullPath(), duplicateXmlNodeNames.toString());
            return false;
        }
        return true;
    }

    private void logOptionalErrorMessage(boolean doLogging, String format, Object... args) {
        if (doLogging) {
            log.error(format, args);
        }
    }

    /**
     * Returns the child nodes of a given datastore node.
     * <p>
     * The child nodes can include nodes that are in the xmlNodes field of the current node, and can
     * also include database nodes that are not included in the xmlNodes field. Other xmlNodes
     * elements can be more remote descendants of the current node (grandchildren, etc.). This
     * method constructs the collection of child nodes to the current node, taking the
     * aforementioned factors into account, and puts any remote descendants from xmlNodes into the
     * xmlNodes fields of the child nodes.
     * <p>
     * In the process of updating the child node population, nodes that were child nodes of the
     * original database node may no longer be children of that node. In that case, the obsolete
     * child nodes must be removed from the database, along with all of their descendants. The node
     * deletion information is also updated as part of this process.
     */
    private List<DatastoreNode> childNodes(DatastoreNode node) {

        // Update the full paths of any child nodes.
        List<String> childNodeNames = childNodeNames(node.getNodes());
        List<String> originalChildNodeFullPaths = node.getChildNodeFullPaths();
        List<String> updatedChildNodeFullPaths = new ArrayList<>();
        for (String childNodeName : childNodeNames) {
            if (!StringUtils.isBlank(childNodeName)) {
                updatedChildNodeFullPaths
                    .add(DatastoreWalker.fullPathFromParentPath(childNodeName, node.getFullPath()));
            }
        }

        // Mark any obsolete child nodes for deletion.
        node.setChildNodeFullPaths(updatedChildNodeFullPaths);
        originalChildNodeFullPaths.removeAll(updatedChildNodeFullPaths);
        updateFullPathsForNodesToRemove(originalChildNodeFullPaths);

        // Set the xmlNode full paths, assuming that they are child nodes of
        // this node (if it turns out they aren't, we'll take care of that later).
        for (DatastoreNode xmlNode : node.getXmlNodes()) {
            xmlNode.setFullPath(fullPathFromParentPath(xmlNode, node.getFullPath()));
        }

        // Obtain the child nodes (which may include nodes from the database that are
        // not included in the imported nodes), and find nodes that might be more
        // remote descendants of the current node.
        List<DatastoreNode> childNodes = locateChildNodes(node, childNodeNames);
        List<DatastoreNode> descendantNodes = new ArrayList<>(node.getXmlNodes());
        descendantNodes.removeAll(childNodes);

        // Add the remote descendants to the xmlNodes of all the children.
        for (DatastoreNode childNode : childNodes) {
            childNode.getXmlNodes().addAll(descendantNodes);
        }

        // Remove the remote descendants from the current node's xmlNodes.
        node.getXmlNodes().removeAll(descendantNodes);
        return childNodes;
    }

    /** Recursively adds datastore node full paths to the list of nodes for removal. */
    private void updateFullPathsForNodesToRemove(List<String> fullPathsForRemoval) {
        if (fullPathsForRemoval.isEmpty()) {
            return;
        }
        for (String fullPathForRemoval : fullPathsForRemoval) {
            updateFullPathsForNodesToRemove(
                databaseDatastoreNodesByFullPath.get(fullPathForRemoval).getChildNodeFullPaths());
            fullPathsForNodesToRemove.add(fullPathForRemoval);
            log.warn("Datastore location {} will be removed from the database", fullPathForRemoval);
        }
    }

    /**
     * Locates the child nodes for the current database node based on their names.
     * <p>
     * The imported nodes, stored in the current node's xmlNodes field, are searched for
     * appropriately-named nodes. Any that are found are added to the child nodes collection. Any
     * missing child nodes are retrieved from the existing database nodes. In this latter case, the
     * database node's child node full paths have to be converted back into a String for that node's
     * nodes field.
     */
    private List<DatastoreNode> locateChildNodes(DatastoreNode node, List<String> childNodeNames) {
        List<DatastoreNode> childNodes = new ArrayList<>();
        for (String childNodeName : childNodeNames) {
            DatastoreNode childNode = null;
            for (DatastoreNode xmlNode : node.getXmlNodes()) {
                if (xmlNode.getName().equals(childNodeName)) {
                    childNode = xmlNode;
                    continue;
                }
            }
            if (childNode == null) {
                childNode = childNodeFromImportedNodes(childNodeName);
            }
            if (childNode == null) {
                childNode = childNodeFromDatabaseNodes(
                    DatastoreWalker.fullPathFromParentPath(childNodeName, node.getFullPath()));
            }
            if (childNode != null) {
                childNodes.add(childNode);
            }
        }
        return childNodes;
    }

    /** Locates a child node from the existing database nodes and updates its fields. */
    private DatastoreNode childNodeFromDatabaseNodes(String childNodeFullPath) {
        DatastoreNode childNode = databaseDatastoreNodesByFullPath.get(childNodeFullPath);
        if (childNode != null) {
            childNode.setNodes(nodesFromChildNodeFullPaths(childNode));
        }
        return childNode;
    }

    private DatastoreNode childNodeFromImportedNodes(String childNodeName) {
        for (DatastoreNode node : importedDatastoreNodes) {
            if (node.getName().equals(childNodeName)) {
                return node;
            }
        }
        return null;
    }

    /** Converts a node's child node full paths to a node string. */
    String nodesFromChildNodeFullPaths(DatastoreNode node) {
        if (node.getChildNodeFullPaths().isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String childNodeFullPath : node.getChildNodeFullPaths()) {
            sb.append(databaseDatastoreNodesByFullPath.get(childNodeFullPath).getName());
            sb.append(DatastoreNode.CHILD_NODE_NAME_DELIMITER);
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /** Converts the nodes field into a {@link List} of child node names. */
    List<String> childNodeNames(String xmlNodesAttribute) {
        List<String> childNodeNames = new ArrayList<>();
        String[] childNodeNamesArray = xmlNodesAttribute
            .split(DatastoreNode.CHILD_NODE_NAME_DELIMITER);
        for (String childNodeName : childNodeNamesArray) {
            childNodeNames.add(childNodeName.trim());
        }
        return childNodeNames;
    }

    /** Checks a {@link List} of {@link DatastoreNode}s for duplicate names. */
    List<String> duplicateXmlNodeNames(List<DatastoreNode> xmlNodes) {
        return ZiggyStringUtils.duplicateStrings(
            xmlNodes.stream().map(DatastoreNode::getName).collect(Collectors.toList()));
    }

    private DatastoreWalker datastoreWalker() {
        if (datastoreWalker == null) {

            Map<String, DatastoreRegexp> regexpByName = datastoreOperations()
                .datastoreRegexpsByName();
            for (DatastoreRegexp regexp : importedDatastoreRegexps) {
                regexpByName.put(regexp.getName(), regexp);
            }
            Map<String, DatastoreNode> datastoreNodesByFullPath = datastoreOperations()
                .datastoreNodesByFullPath();
            for (String fullPathToRemove : fullPathsForNodesToRemove) {
                if (datastoreNodesByFullPath.containsKey(fullPathToRemove)) {
                    datastoreNodesByFullPath.remove(fullPathToRemove);
                }
            }
            for (DatastoreNode nodeForDatabase : nodesForDatabase) {
                datastoreNodesByFullPath.put(nodeForDatabase.getFullPath(), nodeForDatabase);
            }
            datastoreWalker = new DatastoreWalker(regexpByName, datastoreNodesByFullPath);
        }
        return datastoreWalker;
    }

    /** Ensure uniqueness of all imported data file type definitions. */
    private void checkDataFileTypeDefinitions() {
        if (CollectionUtils.isEmpty(importedDataFileTypes)) {
            return;
        }

        // Check for duplicates within the imports.
        Set<String> uniqueDataFileTypeNames = importedDataFileTypes.stream()
            .map(DataFileType::getName)
            .collect(Collectors.toSet());
        if (importedDataFileTypes.size() != uniqueDataFileTypeNames.size()) {
            throw new IllegalStateException(
                "Unable to persist data file types due to duplicate names");
        }

        // Check for invalid locations.
        List<DataFileType> validDataFileTypes = importedDataFileTypes.stream()
            .filter(s -> datastoreWalker().locationExists(s.getLocation()))
            .toList();
        if (validDataFileTypes.size() != importedDataFileTypes.size()) {
            List<String> invalidNames = importedDataFileTypes.stream()
                .filter(s -> !validDataFileTypes.contains(s))
                .map(DataFileType::getName)
                .toList();
            log.warn("Unable to import data file types with invalid locations: {}",
                invalidNames.toString());
            importedDataFileTypes = validDataFileTypes;
        }
    }

    /**
     * Check that all model type definitions are unique and that their database-only fields can be
     * populated without errors.
     */
    private void checkModelTypeDefinitions() {
        if (CollectionUtils.isEmpty(importedModelTypes)) {
            return;
        }

        List<ModelType> modelTypesNotImported = new ArrayList<>();
        for (ModelType modelTypeXb : importedModelTypes) {
            try {
                modelTypeXb.validate();
            } catch (Exception e) {
                log.warn("Unable to validate model type definition {}", modelTypeXb.getType(), e);
                modelTypesNotImported.add(modelTypeXb);
                continue;
            }
        }
        log.info("Imported {} ModelType definitions from files", importedModelTypes.size());

        // Now check for duplicate model names in the imports.
        Set<String> uniqueModelTypeNames = importedModelTypes.stream()
            .map(ModelType::getType)
            .collect(Collectors.toSet());
        if (importedModelTypes.size() != uniqueModelTypeNames.size()) {
            throw new IllegalStateException("Unable to persist model types due to duplicate names");
        }
    }

    DatastoreOperations datastoreOperations() {
        return datastoreOperations;
    }

    public List<DataFileType> getDataFileTypes() {
        return importedDataFileTypes;
    }

    public List<ModelType> getModelTypes() {
        return importedModelTypes;
    }

    public List<DatastoreRegexp> getRegexps() {
        return importedDatastoreRegexps;
    }

    Map<String, DatastoreRegexp> regexpsByName() {
        Map<String, DatastoreRegexp> regexpsByName = new HashMap<>();
        for (DatastoreRegexp regexp : importedDatastoreRegexps) {
            regexpsByName.put(regexp.getName(), regexp);
        }
        return regexpsByName;
    }

    public Set<DatastoreNode> nodesForDatabase() {
        return nodesForDatabase;
    }

    public List<String> getFullPathsForNodesToRemove() {
        return fullPathsForNodesToRemove;
    }

    public Map<String, DatastoreNode> getDatabaseDatastoreNodesByFullPath() {
        return databaseDatastoreNodesByFullPath;
    }

    Map<String, DatastoreNode> nodesByFullPath() {
        Map<String, DatastoreNode> nodesByFullPath = new HashMap<>();
        for (DatastoreNode node : nodesForDatabase) {
            nodesByFullPath.put(node.getFullPath(), node);
        }
        return nodesByFullPath;
    }
}

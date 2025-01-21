/*
 * Copyright (C) 2022-2025 United States Government as represented by the Administrator of the
 * National Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline
 * Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A,
 * 80NSSC18M0068 & 80NSSC21M0079.
 *
 * This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have
 * received a copy of this agreement with the Ziggy source code; see the file LICENSE.pdf.
 *
 * Disclaimers
 *
 * No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER
 * EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT
 * SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE
 * ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT
 * SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY
 * OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY
 * OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY
 * DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE
 * ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."
 *
 * Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES
 * GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S
 * USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES
 * ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM,
 * RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
 * STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE
 * EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE,
 * UNILATERAL TERMINATION OF THIS AGREEMENT.
 */

package gov.nasa.ziggy.data.datastore;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Performs import of the datastore configuration.
 * <p>
 * The datastore configuration consists of the following:
 * <ol>
 * <li>Definition of {@link DatastoreRegexp} instances.
 * <li>Definition of {@link DatastoreNode} instances.
 * <li>Definition of {@link DataFileType} instances.
 * <li>Definition of {@link ModelType} instances.
 * </ol>
 *
 * @author PT
 */
public class DatastoreConfigurationImporter {

    private static final Logger log = LoggerFactory.getLogger(DatastoreConfigurationImporter.class);

    private static final String DRY_RUN_OPTION = "dry-run";
    private static final String HELP_OPTION = "help";

    private List<String> filenames;
    private boolean dryRun;

    private List<DataFileType> importedDataFileTypes = new ArrayList<>();
    private List<ModelType> importedModelTypes = new ArrayList<>();
    private List<DatastoreRegexp> importedDatastoreRegexps = new ArrayList<>();
    private List<DatastoreNode> importedDatastoreNodes = new ArrayList<>();
    private List<String> fullPathsForNodesToRemove = new ArrayList<>();
    private Set<DatastoreNode> nodesForDatabase = new HashSet<>();

    private Map<String, DatastoreNode> databaseDatastoreNodesByFullPath;

    private DatastoreOperations datastoreOperations = new DatastoreOperations();

    // The following are instantiated so that unit tests that rely on them don't fail

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        Options options = new Options()
            .addOption(Option.builder("h").longOpt(HELP_OPTION).desc("Show this help").build())
            .addOption(Option.builder("n")
                .longOpt(DRY_RUN_OPTION)
                .desc("Parses and creates objects but does not persist to database")
                .build());

        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            usageAndExit(options, e.getMessage(), e);
        }

        if (cmdLine.hasOption(HELP_OPTION)) {
            usageAndExit(options, null, null);
        }

        try {
            List<String> filenames = cmdLine.getArgList();
            boolean dryRun = cmdLine.hasOption(DRY_RUN_OPTION);
            new DatastoreConfigurationImporter(filenames, dryRun).importConfiguration();
        } catch (Exception e) {
            usageAndExit(null, e.getMessage(), e);
        }
    }

    private static void usageAndExit(Options options, String message, Throwable e) {
        // Until we've gotten through argument parsing, emit errors to stderr. Once we start the
        // program, we'll be logging and throwing exceptions.
        if (options != null) {
            if (message != null) {
                System.err.println(message);
            }
            new HelpFormatter().printHelp("DatastoreConfigurationImporter [options] [files]",
                "Import datastore configuration from XML file(s)", options, null);
        } else if (e != null) {
            log.error(message, e);
        }

        System.exit(-1);
    }

    public DatastoreConfigurationImporter(List<String> filenames, boolean dryRun) {
        this.filenames = filenames;
        this.dryRun = dryRun;
    }

    /**
     * Perform the import from all XML files. The importer will skip any file that fails to validate
     * or cannot be parsed, will skip any DataFileType instance that fails internal validation, and
     * will skip any DataFileType that has the name of a type that is already in the database; all
     * other DataFileTypes will be imported. If any duplicate names are present in the set of
     * DataFileType instances to be imported, none will be imported. The import also imports model
     * definitions.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void importConfiguration() {

        databaseDatastoreNodesByFullPath = datastoreOperations().datastoreNodesByFullPath();

        for (String filename : filenames) {
            File file = new File(filename);
            if (!file.exists() || !file.isFile()) {
                log.warn("File {} is not a regular file", filename);
                continue;
            }

            // open and read the XML file
            log.info("Reading from {}", filename);
            DatastoreConfigurationFile configDoc = null;
            try {
                configDoc = new ValidatingXmlManager<>(DatastoreConfigurationFile.class)
                    .unmarshal(file);
            } catch (Exception e) {
                log.warn("Unable to parse configuration file {}", filename, e);
                continue;
            }

            log.info("Importing DataFileType definitions from {}", filename);
            importedDataFileTypes.addAll(configDoc.getDataFileTypes());

            log.info("Importing ModelType definitions from {}", filename);
            importedModelTypes.addAll(configDoc.getModelTypes());

            log.info("Importing datastore regexp definitions from {}", filename);
            importedDatastoreRegexps.addAll(configDoc.getRegexps());

            log.info("Importing datastore node definitions from {}", filename);
            importedDatastoreNodes.addAll(configDoc.getDatastoreNodes());
        }
        checkDefinitions();
        if (!dryRun) {
            persistDefinitions();
        } else {
            log.info("Dry run.");
        }
    }

    /**
     * Validates all imports.
     * <p>
     * This method is ordinarily called as part of {@link #importConfiguration()}. It is broken out
     * as a separate, package-scoped method to support testing.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    void checkDefinitions() {

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

            // Generate nodeForDatabase instances for the child nodes.
            nodesForDatabase.add(nodeForDatabase);
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
        DatastoreNode nodeForDatabase = node;
        if (databaseDatastoreNodesByFullPath.get(node.getFullPath()) != null) {
            nodeForDatabase = databaseDatastoreNodesByFullPath.get(node.getFullPath());
            nodeForDatabase.setRegexp(node.isRegexp());
            nodeForDatabase.setNodes(node.getNodes());
            nodeForDatabase.setXmlNodes(node.getXmlNodes());
        }
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

        // Add full path values to the XML nodes, assuming that the XML
        // nodes are all children of the current node.
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

    /** Ensure uniqueness of all imported data file type definitions. */
    private void checkDataFileTypeDefinitions() {

        // First check against the database definitions.
        List<DataFileType> dataFileTypesNotImported = new ArrayList<>();
        List<String> databaseDataFileTypeNames = datastoreOperations().dataFileTypeNames();
        for (DataFileType typeXb : importedDataFileTypes) {
            if (databaseDataFileTypeNames.contains(typeXb.getName())) {
                log.warn(
                    "Not importing data file type definition {} due to presence of existing type with same name",
                    typeXb.getName());
                dataFileTypesNotImported.add(typeXb);
                continue;
            }
        }
        importedDataFileTypes.removeAll(dataFileTypesNotImported);

        // Now check for duplicates within the imports.
        Set<String> uniqueDataFileTypeNames = importedDataFileTypes.stream()
            .map(DataFileType::getName)
            .collect(Collectors.toSet());
        if (importedDataFileTypes.size() != uniqueDataFileTypeNames.size()) {
            throw new IllegalStateException(
                "Unable to persist data file types due to duplicate names");
        }
    }

    /**
     * Check that all model type definitions are unique and that their database-only fields can be
     * populated without errors.
     */
    private void checkModelTypeDefinitions() {
        List<ModelType> modelTypesNotImported = new ArrayList<>();
        List<String> databaseModelTypes = datastoreOperations().modelTypes();
        for (ModelType modelTypeXb : importedModelTypes) {
            try {
                modelTypeXb.validate();
            } catch (Exception e) {
                log.warn("Unable to validate model type definition {}", modelTypeXb.getType(), e);
                modelTypesNotImported.add(modelTypeXb);
                continue;
            }
            if (databaseModelTypes.contains(modelTypeXb.getType())) {
                log.warn(
                    "Not importing model type definition {} due to presence of existing type with same name",
                    modelTypeXb.getType());
                modelTypesNotImported.add(modelTypeXb);
                continue;
            }
        }
        importedModelTypes.removeAll(modelTypesNotImported);
        log.info("Imported {} ModelType definitions from files", importedModelTypes.size());

        // Now check for duplicate model names in the imports.
        Set<String> uniqueModelTypeNames = importedModelTypes.stream()
            .map(ModelType::getType)
            .collect(Collectors.toSet());
        if (importedModelTypes.size() != uniqueModelTypeNames.size()) {
            throw new IllegalStateException("Unable to persist model types due to duplicate names");
        }
    }

    /** Persist all definitions to the database. */
    private void persistDefinitions() {
        Set<DatastoreNode> datastoreNodesToRemove = new HashSet<>();
        for (String fullPathForNodeToRemove : fullPathsForNodesToRemove) {
            datastoreNodesToRemove
                .add(databaseDatastoreNodesByFullPath.get(fullPathForNodeToRemove));
        }
        datastoreOperations().persistDatastoreConfiguration(importedDataFileTypes,
            importedModelTypes, importedDatastoreRegexps, datastoreNodesToRemove, nodesForDatabase,
            log);
    }

    DatastoreOperations datastoreOperations() {
        return datastoreOperations;
    }

    List<DataFileType> getDataFileTypes() {
        return importedDataFileTypes;
    }

    List<ModelType> getModelTypes() {
        return importedModelTypes;
    }

    List<DatastoreRegexp> getRegexps() {
        return importedDatastoreRegexps;
    }

    Map<String, DatastoreRegexp> regexpsByName() {
        Map<String, DatastoreRegexp> regexpsByName = new HashMap<>();
        for (DatastoreRegexp regexp : importedDatastoreRegexps) {
            regexpsByName.put(regexp.getName(), regexp);
        }
        return regexpsByName;
    }

    Set<DatastoreNode> nodesForDatabase() {
        return nodesForDatabase;
    }

    List<String> getFullPathsForNodesToRemove() {
        return fullPathsForNodesToRemove;
    }

    Map<String, DatastoreNode> nodesByFullPath() {
        Map<String, DatastoreNode> nodesByFullPath = new HashMap<>();
        for (DatastoreNode node : nodesForDatabase) {
            nodesByFullPath.put(node.getFullPath(), node);
        }
        return nodesByFullPath;
    }
}

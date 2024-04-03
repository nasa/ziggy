package gov.nasa.ziggy.ui.util.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.netbeans.swing.outline.Outline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Groupable;
import gov.nasa.ziggy.ui.util.GroupInformation;
import gov.nasa.ziggy.ui.util.proxy.RetrieveLatestVersionsCrudProxy;

/**
 * Implements a generic tree model for use with {@link Outline} for display of hierarchical tables.
 * <p>
 * This class organizes a set of database objects by the value of their {@link Group} field. All
 * objects are initially assigned to the default group. The user may reassign objects to other
 * groups. The table display will show the tree of defined groups, and allow the user to expand or
 * collapse groups. In this way, it is possible to display a large number of objects in a table
 * while preventing the display from becoming unwieldy.
 *
 * @author Todd Klaus
 * @author PT
 */
public class ZiggyTreeModel<T extends Groupable> extends DefaultTreeModel
    implements ConsoleDatabaseModel {

    private static final long serialVersionUID = 20230511L;

    private static final Logger log = LoggerFactory.getLogger(ZiggyTreeModel.class);

    private List<T> defaultGroup = new LinkedList<>();
    private Map<Group, List<T>> groups = new HashMap<>();
    private Map<String, T> objectsByName = new HashMap<>();

    private final RetrieveLatestVersionsCrudProxy<T> crudProxy;

    private final DefaultMutableTreeNode rootNode;
    private DefaultMutableTreeNode defaultGroupNode;
    private Map<String, DefaultMutableTreeNode> groupNodes;

    private Class<T> modelClass;

    private boolean modelValid = false;

    public ZiggyTreeModel(RetrieveLatestVersionsCrudProxy<T> crudProxy, Class<T> modelClass) {
        super(new DefaultMutableTreeNode(""));
        rootNode = (DefaultMutableTreeNode) getRoot();
        this.crudProxy = crudProxy;
        this.modelClass = modelClass;
        DatabaseModelRegistry.registerModel(this);
    }

    public void loadFromDatabase() throws PipelineException {
        if (groups != null) {
            log.debug("Clearing the Hibernate cache of all loaded pipelines");
            for (List<T> objects : groups.values()) {
                crudProxy.evictAll(objects); // clear the cache
            }
        }

        if (defaultGroup != null) {
            crudProxy.evictAll(defaultGroup); // clear the cache
        }

        // Obtain information on the groups for this component class.
        GroupInformation<T> groupInformation = new GroupInformation<>(modelClass,
            crudProxy.retrieveLatestVersions());
        objectsByName = groupInformation.getObjectsByName();

        // Add the default group.
        rootNode.removeAllChildren();
        defaultGroupNode = new DefaultMutableTreeNode("<Default Group>");
        insertNodeInto(defaultGroupNode, rootNode, rootNode.getChildCount());

        defaultGroup = groupInformation.getDefaultGroup();
        Collections.sort(defaultGroup, Comparator.comparing(Object::toString));

        for (T object : defaultGroup) {
            DefaultMutableTreeNode pipelineNode = new DefaultMutableTreeNode(object);
            insertNodeInto(pipelineNode, defaultGroupNode, defaultGroupNode.getChildCount());
        }

        // Add the rest of the groups alphabetically.
        groups = groupInformation.getGroups();
        List<Group> groupsList = new ArrayList<>(groups.keySet());
        Collections.sort(groupsList, Comparator.comparing(Group::getName));
        groupNodes = new HashMap<>();

        for (Group group : groupsList) {
            DefaultMutableTreeNode groupNode = new DefaultMutableTreeNode(group.getName());
            insertNodeInto(groupNode, rootNode, rootNode.getChildCount());
            groupNodes.put(group.getName(), groupNode);

            List<T> objects = groups.get(group);
            Collections.sort(objects, Comparator.comparing(Object::toString));

            for (T object : objects) {
                DefaultMutableTreeNode treeNode = new DefaultMutableTreeNode(object);
                insertNodeInto(treeNode, groupNode, groupNode.getChildCount());
            }
        }

        reload();
        modelValid = true;
        log.debug("Done loading");
    }

    public DefaultMutableTreeNode groupNode(String groupName) {
        return groupNodes.get(groupName);
    }

    public Map<String, DefaultMutableTreeNode> getGroupNodes() {
        return groupNodes;
    }

    /** Returns an object based on its name, or null if no object exists with that name. */
    public T objectByName(String name) {
        return objectsByName.get(name);
    }

    @Override
    public void invalidateModel() {
        modelValid = false;
    }

    /**
     * Reload the model if it has been marked invalid Should only be called by a RowModel
     */
    public void validityCheck() {
        if (!modelValid) {
            log.info("Model invalid for " + this.getClass().getSimpleName()
                + ", loading data from database...");
            loadFromDatabase();
        }
    }

    public DefaultMutableTreeNode getDefaultGroupNode() {
        return defaultGroupNode;
    }

    public DefaultMutableTreeNode getRootNode() {
        return rootNode;
    }
}

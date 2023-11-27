package gov.nasa.ziggy.ui.util.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.netbeans.swing.outline.Outline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.HasGroup;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
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
public class ZiggyTreeModel<T extends HasGroup> extends DefaultTreeModel
    implements ConsoleDatabaseModel {

    private static final long serialVersionUID = 20230511L;

    private static final Logger log = LoggerFactory.getLogger(ZiggyTreeModel.class);

    private List<T> defaultGroup = new LinkedList<>();
    private Map<Group, List<T>> groups = new HashMap<>();
    private Map<String, T> objectsByGroupName = new HashMap<>();

    private final RetrieveLatestVersionsCrudProxy<T> crudProxy;

    private final DefaultMutableTreeNode rootNode;
    private DefaultMutableTreeNode defaultGroupNode;
    private Map<String, DefaultMutableTreeNode> groupNodes;

    private boolean modelValid = false;

    public ZiggyTreeModel(RetrieveLatestVersionsCrudProxy<T> crudProxy) {
        super(new DefaultMutableTreeNode(""));
        rootNode = (DefaultMutableTreeNode) getRoot();
        this.crudProxy = crudProxy;
        DatabaseModelRegistry.registerModel(this);
    }

    public void loadFromDatabase() throws PipelineException {
        List<T> allObjects = null;

        try {
            if (groups != null) {
                log.debug("Clearing the Hibernate cache of all loaded pipelines");
                for (List<T> objects : groups.values()) {
                    crudProxy.evictAll(objects); // clear the cache
                }
            }

            if (defaultGroup != null) {
                crudProxy.evictAll(defaultGroup); // clear the cache
            }

            defaultGroup = new LinkedList<>();
            groups = new HashMap<>();
            objectsByGroupName = new HashMap<>();
            groupNodes = new HashMap<>();

            allObjects = crudProxy.retrieveLatestVersions();
        } catch (ConsoleSecurityException ignore) {
            return;
        }

        for (T object : allObjects) {
            objectsByGroupName.put(object.groupName(), object);

            Group group = object.group();

            if (group == null) {
                // default group
                defaultGroup.add(object);
            } else {
                List<T> groupList = groups.get(group);

                if (groupList == null) {
                    groupList = new LinkedList<>();
                    groups.put(group, groupList);
                }

                groupList.add(object);
            }
        }

        // create the tree
        rootNode.removeAllChildren();
        defaultGroupNode = new DefaultMutableTreeNode("<Default Group>");
        insertNodeInto(defaultGroupNode, rootNode, rootNode.getChildCount());

        for (T object : defaultGroup) {
            DefaultMutableTreeNode pipelineNode = new DefaultMutableTreeNode(object);
            insertNodeInto(pipelineNode, defaultGroupNode, defaultGroupNode.getChildCount());
        }

        // sort groups alphabetically

        Set<Group> groupsSet = groups.keySet();
        List<Group> groupsList = new ArrayList<>(groupsSet);
        Collections.sort(groupsList, Comparator.comparing(Group::getName));

        for (Group group : groupsList) {
            DefaultMutableTreeNode groupNode = new DefaultMutableTreeNode(group.getName());
            insertNodeInto(groupNode, rootNode, rootNode.getChildCount());
            groupNodes.put(group.getName(), groupNode);

            List<T> objects = groups.get(group);

            for (T object : objects) {
                DefaultMutableTreeNode pipelineNode = new DefaultMutableTreeNode(object);
                insertNodeInto(pipelineNode, groupNode, groupNode.getChildCount());
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

    /**
     * Returns true if an object already exists with the specified name. checked when the operator
     * changes the pipeline name so we can warn them before we get a database constraint violation.
     *
     * @param name
     * @return
     */
    public T pipelineByName(String name) {
        return objectsByGroupName.get(name);
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

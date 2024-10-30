package gov.nasa.ziggy.ui.util.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import javax.swing.SwingWorker;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.netbeans.swing.outline.Outline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.ui.util.GroupInformation;

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
 * @author Bill Wohler
 */
public class ZiggyTreeModel<T> extends DefaultTreeModel {

    private static final long serialVersionUID = 20240614L;
    private static final Logger log = LoggerFactory.getLogger(ZiggyTreeModel.class);

    private final DefaultMutableTreeNode rootNode;
    private DefaultMutableTreeNode defaultGroupNode;
    private Map<String, DefaultMutableTreeNode> groupNodes;

    private Supplier<List<T>> items;
    private String type;

    public ZiggyTreeModel(String type, Supplier<List<T>> items) {
        super(new DefaultMutableTreeNode(""));
        rootNode = (DefaultMutableTreeNode) getRoot();
        this.type = type;
        this.items = items;
    }

    public void loadFromDatabase() throws PipelineException {
        new SwingWorker<GroupInformation<T>, Void>() {

            @Override
            protected GroupInformation<T> doInBackground() throws Exception {
                // Obtain information on the groups for this component class.
                log.debug("Loading {} items", type);
                return new GroupInformation<>(type, items.get());
            }

            @Override
            protected void done() {
                GroupInformation<T> groupInformation;
                try {
                    groupInformation = get();
                    log.debug("Loading {} items...done", type);

                    // Add the default group.
                    log.debug("Updating tree model for {}", type);
                    rootNode.removeAllChildren();
                    defaultGroupNode = new DefaultMutableTreeNode(Group.DEFAULT_NAME);
                    insertNodeInto(defaultGroupNode, rootNode, rootNode.getChildCount());

                    Collections.sort(groupInformation.getObjectsInDefaultGroup(),
                        Comparator.comparing(Object::toString));

                    for (T object : groupInformation.getObjectsInDefaultGroup()) {
                        DefaultMutableTreeNode pipelineNode = new DefaultMutableTreeNode(object);
                        insertNodeInto(pipelineNode, defaultGroupNode,
                            defaultGroupNode.getChildCount());
                    }

                    // Add the rest of the groups alphabetically.
                    List<Group> groupsList = new ArrayList<>(
                        groupInformation.getObjectsByGroup().keySet());
                    Collections.sort(groupsList, Comparator.comparing(Group::getName));
                    groupNodes = new HashMap<>();

                    for (Group group : groupsList) {
                        DefaultMutableTreeNode groupNode = new DefaultMutableTreeNode(
                            group.getName());
                        insertNodeInto(groupNode, rootNode, rootNode.getChildCount());
                        groupNodes.put(group.getName(), groupNode);

                        List<T> objects = groupInformation.getObjectsByGroup().get(group);
                        Collections.sort(objects, Comparator.comparing(Object::toString));

                        for (T object : objects) {
                            DefaultMutableTreeNode treeNode = new DefaultMutableTreeNode(object);
                            insertNodeInto(treeNode, groupNode, groupNode.getChildCount());
                        }
                    }

                    reload();

                    log.debug("Updating tree model for {}...done", type);
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Could not retrieve model data for {}", type, e);
                }
            }
        }.execute();
    }

    public Map<String, DefaultMutableTreeNode> getGroupNodes() {
        return groupNodes;
    }

    public DefaultMutableTreeNode getDefaultGroupNode() {
        return defaultGroupNode;
    }

    public DefaultMutableTreeNode getRootNode() {
        return rootNode;
    }
}

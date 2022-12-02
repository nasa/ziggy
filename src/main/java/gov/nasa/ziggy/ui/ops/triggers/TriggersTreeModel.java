package gov.nasa.ziggy.ui.ops.triggers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.tree.AbstractLayoutCache;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.netbeans.swing.outline.Outline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.ConsoleDatabaseModel;
import gov.nasa.ziggy.ui.models.DatabaseModelRegistry;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * Tree model that groups by Group. For use in Outline.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TriggersTreeModel extends DefaultTreeModel implements ConsoleDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(TriggersTreeModel.class);

    private List<PipelineDefinition> defaultGroupTriggers = new LinkedList<>();
    private Map<Group, List<PipelineDefinition>> triggers = new HashMap<>();
    private Map<String, PipelineDefinition> triggersByName = new HashMap<>();

    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    private final DefaultMutableTreeNode rootNode;
    private DefaultMutableTreeNode defaultGroupNode;
    private Map<String, DefaultMutableTreeNode> groupNodes;

    private boolean modelValid = false;

    private HashMap<String, Boolean> expansionState;
    private boolean defaultGroupExpansionState;

    private Outline triggersOutline;

    public TriggersTreeModel() {
        super(new DefaultMutableTreeNode(""));
        rootNode = (DefaultMutableTreeNode) getRoot();
        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();
        DatabaseModelRegistry.registerModel(this);
    }

    public Outline getTriggersOutline() {
        return triggersOutline;
    }

    public void setTriggersOutline(Outline triggersOutline) {
        this.triggersOutline = triggersOutline;
    }

    public void loadFromDatabase() throws PipelineException {
        recordExpansionState();
        reloadModel();
        applyExpansionState();

        modelValid = true;
    }

    private void reloadModel() {
        List<PipelineDefinition> allTriggers = null;

        try {
            if (triggers != null) {
                log.debug("Clearing the Hibernate cache of all loaded triggers");
                for (List<PipelineDefinition> pipelineList : triggers.values()) {
                    pipelineDefinitionCrud.evictAll(pipelineList); // clear the cache
                }
            }

            if (defaultGroupTriggers != null) {
                pipelineDefinitionCrud.evictAll(defaultGroupTriggers); // clear the cache
            }

            defaultGroupTriggers = new LinkedList<>();
            triggers = new HashMap<>();
            triggersByName = new HashMap<>();
            groupNodes = new HashMap<>();

            allTriggers = pipelineDefinitionCrud.retrieveLatestVersions();
        } catch (ConsoleSecurityException ignore) {
            return;
        }

        for (PipelineDefinition trigger : allTriggers) {
            triggersByName.put(trigger.getName().getName(), trigger);

            Group group = trigger.getGroup();

            if (group == null) {
                // default group
                defaultGroupTriggers.add(trigger);
            } else {
                List<PipelineDefinition> groupList = triggers.get(group);

                if (groupList == null) {
                    groupList = new LinkedList<>();
                    triggers.put(group, groupList);
                }

                groupList.add(trigger);
            }
        }

        // create the tree
        rootNode.removeAllChildren();
        defaultGroupNode = new DefaultMutableTreeNode("<Default Group>");
        insertNodeInto(defaultGroupNode, rootNode, rootNode.getChildCount());

        for (PipelineDefinition trigger : defaultGroupTriggers) {
            DefaultMutableTreeNode triggerNode = new DefaultMutableTreeNode(trigger);
            insertNodeInto(triggerNode, defaultGroupNode, defaultGroupNode.getChildCount());
        }

        // sort groups alphabetically

        Set<Group> groupsSet = triggers.keySet();
        List<Group> groupsList = new ArrayList<>();
        groupsList.addAll(groupsSet);
        Collections.sort(groupsList, (o1, o2) -> o1.getName().compareTo(o2.getName()));

        for (Group group : groupsList) {
            DefaultMutableTreeNode groupNode = new DefaultMutableTreeNode(group.getName());
            insertNodeInto(groupNode, rootNode, rootNode.getChildCount());
            groupNodes.put(group.getName(), groupNode);

            List<PipelineDefinition> groupTriggers = triggers.get(group);

            for (PipelineDefinition trigger : groupTriggers) {
                DefaultMutableTreeNode triggerNode = new DefaultMutableTreeNode(trigger);
                insertNodeInto(triggerNode, groupNode, groupNode.getChildCount());
            }
        }

        reload();

        log.debug("triggersTreeModel: done loading");
    }

    public DefaultMutableTreeNode groupNode(String groupName) {
        return groupNodes.get(groupName);
    }

    public Map<String, DefaultMutableTreeNode> getGroupNodes() {
        return groupNodes;
    }

    /**
     * Returns true if a trigger already exists with the specified name. checked when the operator
     * changes the trigger name so we can warn them before we get a database constraint violation.
     *
     * @param name
     * @return
     */
    public PipelineDefinition triggerByName(String name) {
        return triggersByName.get(name);
    }

    @Override
    public void invalidateModel() {
        modelValid = false;
    }

    /**
     * Reload the model if it has been marked invalid Should only be called by TriggersRowModel
     */
    void validityCheck() {
        if (!modelValid) {
            log.debug("Model invalid for " + this.getClass().getSimpleName()
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

    private void recordExpansionState() {
        expansionState = new HashMap<>();
        if (triggersOutline != null) {
            AbstractLayoutCache layoutCache = triggersOutline.getLayoutCache();

            Map<String, DefaultMutableTreeNode> groupNodes = getGroupNodes();
            for (String groupName : groupNodes.keySet()) {
                DefaultMutableTreeNode node = groupNodes.get(groupName);
                boolean isExpanded = layoutCache.isExpanded(new TreePath(node.getPath()));

                expansionState.put(groupName, isExpanded);
            }

            defaultGroupExpansionState = layoutCache
                .isExpanded(new TreePath(getDefaultGroupNode().getPath()));
        }
    }

    private void applyExpansionState() {
        if (triggersOutline != null) {
            Map<String, DefaultMutableTreeNode> groupNodes = getGroupNodes();

            for (String groupName : expansionState.keySet()) {
                DefaultMutableTreeNode node = groupNodes.get(groupName);
                if (node != null) {
                    boolean shouldExpand = expansionState.get(groupName);
                    if (shouldExpand) {
                        triggersOutline.expandPath(new TreePath(node.getPath()));
                    } else {
                        triggersOutline.collapsePath(new TreePath(node.getPath()));
                    }
                }
            }

            if (defaultGroupExpansionState) {
                triggersOutline.expandPath(new TreePath(getDefaultGroupNode().getPath()));
            } else {
                triggersOutline.collapsePath(new TreePath(getDefaultGroupNode().getPath()));
            }
        }
    }
}

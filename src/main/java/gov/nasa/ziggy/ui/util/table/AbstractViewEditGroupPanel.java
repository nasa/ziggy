package gov.nasa.ziggy.ui.util.table;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ASSIGN_GROUP;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.COLLAPSE_ALL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPAND_ALL;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.event.ActionEvent;
import java.util.List;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.apache.commons.collections4.CollectionUtils;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.database.GroupOperations;
import gov.nasa.ziggy.ui.util.GroupInformation;
import gov.nasa.ziggy.ui.util.GroupsDialog;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;

/**
 * Extension of {@link AbstractViewEditPanel} that allows for the grouping of objects and for
 * viewing in a tree control.
 *
 * @author PT
 */
public abstract class AbstractViewEditGroupPanel<T> extends AbstractViewEditPanel<T> {

    private static final long serialVersionUID = 20241004L;

    private final GroupOperations groupOperations = new GroupOperations();

    public AbstractViewEditGroupPanel(RowModel rowModel, ZiggyTreeModel<?> treeModel,
        String nodesColumnLabel) {
        super(rowModel, treeModel, nodesColumnLabel);
    }

    @Override
    protected List<JButton> buttons() {
        List<JButton> buttons = super.buttons();
        buttons.addAll(List.of(createButton(EXPAND_ALL, this::expandAll),
            createButton(COLLAPSE_ALL, this::collapseAll)));
        return buttons;
    }

    private void expandAll(ActionEvent evt) {
        ziggyTable.expandAll();
    }

    private void collapseAll(ActionEvent evt) {
        ziggyTable.collapseAll();
    }

    @Override
    protected List<JMenuItem> menuItems() {
        List<JMenuItem> menuItems = super.menuItems();
        menuItems.addAll(List.of(groupMenuItem()));
        return menuItems;
    }

    @SuppressWarnings("serial")
    private JMenuItem groupMenuItem() {
        return createMenuItem(ASSIGN_GROUP + DIALOG, new AbstractAction(ASSIGN_GROUP, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    group();
                } catch (Exception e) {
                    MessageUtils.showError(
                        SwingUtilities.getWindowAncestor(AbstractViewEditGroupPanel.this), e);
                }
            }
        });
    }

    protected abstract String getType();

    /**
     * Assign objects in the table to a selected {@link Group}.
     */
    private void group() {
        Group group = GroupsDialog.selectGroup(this, getType());
        if (group == null) {
            return;
        }
        List<T> selectedObjects = ziggyTable.getContentAtSelectedRows();
        if (selectedObjects.isEmpty()) {
            throw new UnsupportedOperationException("Grouping not permitted");
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                updateGroups(selectedObjects, group);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception

                    // Not to worry--this calls ZiggyTreeModel.loadFromDatabase(), which does its
                    // work in a SwingWorker.
                    ziggyTable.loadFromDatabase();
                } catch (Exception e) {
                    MessageUtils.showError(AbstractViewEditGroupPanel.this, e);
                }
            }
        }.execute();
    }

    /**
     * Updates information in the new group and the groups that formerly held the objects in
     * question.
     */
    private void updateGroups(List<T> objects, Group group) {
        if (CollectionUtils.isEmpty(objects)) {
            return;
        }

        Group databaseGroup = groupOperations().group(group.getName(), group.getType());
        GroupInformation<T> groupInformation = new GroupInformation<>(getType(), objects);
        for (T object : objects) {
            Set<String> items = groupInformation.getGroupByObject().get(object).getItems();
            if (!CollectionUtils.isEmpty(items)) {
                items.remove(object.toString());
            }
            if (databaseGroup != Group.DEFAULT) {
                databaseGroup.getItems().add(object.toString());
            }
        }
        if (databaseGroup != Group.DEFAULT) {
            groupOperations().merge(databaseGroup);
        }
        Set<Group> groups = groupInformation.getObjectsByGroup().keySet();
        groups.remove(databaseGroup); // avoid merging stale information
        groupOperations().merge(groups);
    }

    private GroupOperations groupOperations() {
        return groupOperations;
    }
}

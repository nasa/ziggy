package gov.nasa.ziggy.ui.util.table;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ASSIGN_GROUP;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.COLLAPSE_ALL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPAND_ALL;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.event.ActionEvent;
import java.util.List;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.SwingUtilities;

import org.apache.commons.collections.CollectionUtils;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Groupable;
import gov.nasa.ziggy.ui.util.GroupInformation;
import gov.nasa.ziggy.ui.util.GroupsDialog;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.proxy.GroupCrudProxy;

/**
 * Extension of {@link AbstractViewEditPanel} for classes of objects that extend {@link Groupable},
 * and hence need some mechanism by which their groups can be configured.
 * <p>
 * The addition of this class was necessary because I couldn't figure out a way to explain to the
 * parent class that when we're using the grouping methods in {@link Groupable}; but otherwise, it
 * might or might not.
 *
 * @author PT
 */
public abstract class AbstractViewEditGroupPanel<T extends Groupable>
    extends AbstractViewEditPanel<T> {

    private static final long serialVersionUID = 20231112L;

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
        menuItems.addAll(List.of(getGroupMenuItem()));
        return menuItems;
    }

    @SuppressWarnings("serial")
    private JMenuItem getGroupMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(ASSIGN_GROUP + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    group();
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    /**
     * Assign objects in the table to a selected {@link Group}.
     */
    protected void group() {
        try {
            Group group = GroupsDialog.selectGroup(this);
            if (group == null) {
                return;
            }
            List<T> selectedObjects = ziggyTable.getContentAtSelectedRows();
            if (selectedObjects.isEmpty()) {
                throw new UnsupportedOperationException("Grouping not permitted");
            }
            updateGroups(selectedObjects, group);
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    /**
     * Updates information in the new group and the groups that formerly held the objects in
     * question.
     */
    @SuppressWarnings("unchecked")
    private void updateGroups(List<T> objects, Group group) {
        if (CollectionUtils.isEmpty(objects)) {
            return;
        }
        GroupCrudProxy crudProxy = new GroupCrudProxy();
        Group databaseGroup = crudProxy.retrieveGroupByName(group.getName(),
            objects.get(0).getClass());
        GroupInformation<T> groupInformation = new GroupInformation<>(
            (Class<T>) objects.get(0).getClass(), objects);
        for (T object : objects) {
            Set<String> memberNames = groupInformation.getObjectGroups()
                .get(object)
                .getMemberNames();
            if (!CollectionUtils.isEmpty(memberNames)) {
                memberNames.remove(object.getName());
            }
            if (databaseGroup != Group.DEFAULT) {
                databaseGroup.getMemberNames().add(object.getName());
            }
        }
        if (databaseGroup != Group.DEFAULT) {
            crudProxy.merge(databaseGroup);
        }
        crudProxy.merge(groupInformation.getGroups().keySet());
    }
}

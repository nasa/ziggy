package gov.nasa.ziggy.ui.ops.triggers;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.WindowConstants;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreePath;

import org.netbeans.swing.outline.DefaultOutlineModel;
import org.netbeans.swing.outline.Outline;
import org.netbeans.swing.outline.OutlineModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

@SuppressWarnings("serial")
public class OpsTriggersPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(OpsTriggersPanel.class);

    private JScrollPane tableScrollPane;
    private JPopupMenu triggerTablePopupMenu;
    private JMenuItem editMenuItem;
    private JButton refreshButton;
    private JMenuItem deleteMenuItem;
    private JMenuItem cloneMenuItem;
    private JButton fireButton;
    private JButton newButton;
    private JPanel buttonPanel;

    private OutlineModel triggersOutlineModel;
    private TriggersTreeModel triggersTreeModel;

    private int selectedModelRow;

    private Outline triggersOutline;

    private JMenuItem groupAssignMenuItem;

    private JButton collapseAllButton;
    private JButton expandAllButton;

    public OpsTriggersPanel() {
        super();
        initGUI();
    }

    private void expandAllButtonActionPerformed(ActionEvent evt) {
        log.debug("expandAllButton.actionPerformed, event=" + evt);

        DefaultMutableTreeNode rootNode = triggersTreeModel.getRootNode();
        int numKids = rootNode.getChildCount();
        for (int kidIndex = 0; kidIndex < numKids; kidIndex++) {
            DefaultMutableTreeNode kid = (DefaultMutableTreeNode) rootNode.getChildAt(kidIndex);
            triggersOutline.expandPath(new TreePath(kid.getPath()));
        }
    }

    private void collapseAllButtonActionPerformed(ActionEvent evt) {
        log.debug("collapseAllButton.actionPerformed, event=" + evt);

        DefaultMutableTreeNode rootNode = triggersTreeModel.getRootNode();
        int numKids = rootNode.getChildCount();
        for (int kidIndex = 0; kidIndex < numKids; kidIndex++) {
            DefaultMutableTreeNode kid = (DefaultMutableTreeNode) rootNode.getChildAt(kidIndex);
            triggersOutline.collapsePath(new TreePath(kid.getPath()));
        }
    }

    private void newButtonActionPerformed(ActionEvent evt) {
        log.debug("newButtonActionPerformed(ActionEvent) - start");

        log.debug("newButton.actionPerformed, event=" + evt);

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        NewTriggerDialog newTriggerDialog = ZiggyGuiConsole.newNewTriggerDialog();
        newTriggerDialog.setVisible(true);

        if (!newTriggerDialog.isCancelled()) {
            PipelineDefinition pipelineDefinition = newTriggerDialog.getPipelineDefinition();

            EditTriggerDialog editDialog = ZiggyGuiConsole.newEditTriggerDialog(pipelineDefinition,
                triggersTreeModel);
            editDialog.setVisible(true);

            try {
                triggersTreeModel.loadFromDatabase();
            } catch (PipelineException e) {
                log.error("newButtonActionPerformed(ActionEvent)", e);

                MessageUtil.showError(this, e);
            }
        }

        log.debug("newButtonActionPerformed(ActionEvent) - end");
    }

    private void fireButtonActionPerformed() {
        log.debug("fireButtonActionPerformed(ActionEvent) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        int selectedRow = triggersOutline.getSelectedRow();
        int modelIndex = triggersOutline.convertRowIndexToModel(selectedRow);
        PipelineDefinition trigger = null;
        if (selectedRow != -1) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) triggersOutlineModel
                .getValueAt(modelIndex, 0);
            Object userObject = node.getUserObject();
            if (userObject instanceof PipelineDefinition) {
                trigger = (PipelineDefinition) userObject;
            }
        }

        if (trigger != null) {
            FireTriggerDialog fireTriggerDialog = ZiggyGuiConsole.newFireTriggerDialog(trigger);
            fireTriggerDialog.setVisible(true); // modal, blocks until user
            // dismisses
        }

        log.debug("fireButtonActionPerformed(ActionEvent) - end");
    }

    private void triggerTableMouseClicked(MouseEvent evt) {
        log.debug("triggerTableMouseClicked(MouseEvent) - start");

        log.debug("triggerTable.mouseClicked, event=" + evt);

        if (evt.getClickCount() == 2) {
            log.debug("[DOUBLE-CLICK] table.mouseClicked, event=" + evt);
            selectedModelRow = triggersOutline
                .convertRowIndexToModel(triggersOutline.rowAtPoint(evt.getPoint()));

            log.debug("table row =" + selectedModelRow);

            doEdit();
        }

        log.debug("triggerTableMouseClicked(MouseEvent) - end");
    }

    private void doEdit() {
        log.debug("doEdit(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        int selectedRow = triggersOutline.getSelectedRow();
        int modelIndex = triggersOutline.convertRowIndexToModel(selectedRow);
        if (selectedRow != -1) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) triggersOutlineModel
                .getValueAt(modelIndex, 0);
            Object userObject = node.getUserObject();
            if (userObject instanceof PipelineDefinition) {
                PipelineDefinition trigger = (PipelineDefinition) userObject;
                EditTriggerDialog editDialog = ZiggyGuiConsole.newEditTriggerDialog(trigger,
                    triggersTreeModel);
                editDialog.setVisible(true);

                try {
                    triggersTreeModel.loadFromDatabase();
                } catch (Exception e) {
                    log.error("showEditDialog(User)", e);

                    MessageUtil.showError(this, e);
                }
            }
        }

        log.debug("doEdit(int) - end");
    }

    private void editMenuItemActionPerformed(ActionEvent evt) {
        log.debug("editMenuItem.actionPerformed, event=" + evt);

        doEdit();
    }

    private void cloneMenuItemActionPerformed(ActionEvent evt) {
        log.debug("cloneMenuItem.actionPerformed, event=" + evt);

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        int selectedRow = triggersOutline.getSelectedRow();
        int modelIndex = triggersOutline.convertRowIndexToModel(selectedRow);
        if (selectedRow != -1) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) triggersOutlineModel
                .getValueAt(modelIndex, 0);
            Object userObject = node.getUserObject();
            if (userObject instanceof PipelineDefinition) {
                PipelineDefinition trigger = (PipelineDefinition) node.getUserObject();
                PipelineDefinition clonedTrigger = new PipelineDefinition(trigger);

                EditTriggerDialog editDialog = ZiggyGuiConsole.newEditTriggerDialog(clonedTrigger,
                    triggersTreeModel);
                editDialog.setVisible(true);

                try {
                    triggersTreeModel.loadFromDatabase();
                } catch (Exception e) {
                    log.error("showEditDialog(User)", e);

                    MessageUtil.showError(this, e);
                }
            }
        }
    }

    private void deleteMenuItemActionPerformed(ActionEvent evt) {
        log.debug("deleteMenuItem.actionPerformed, event=" + evt);

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        int selectedRow = triggersOutline.getSelectedRow();
        int modelIndex = triggersOutline.convertRowIndexToModel(selectedRow);
        PipelineDefinition trigger = null;
        if (selectedRow != -1) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) triggersOutlineModel
                .getValueAt(modelIndex, 0);
            Object userObject = node.getUserObject();

            if (userObject instanceof PipelineDefinition) {
                trigger = (PipelineDefinition) userObject;
            }
        }

        if (trigger != null) {
            int choice = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete trigger: = " + trigger.getName() + "?");

            if (choice == JOptionPane.YES_OPTION) {
                try {
                    PipelineDefinitionCrudProxy pipelineCrud = new PipelineDefinitionCrudProxy();
                    pipelineCrud.delete(trigger);
                    triggersTreeModel.loadFromDatabase();
                } catch (Exception e) {
                    MessageUtil.showError(this, e);
                }
            }
        }
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        log.debug("refreshButton.actionPerformed, event=" + evt);
        try {
            triggersTreeModel.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void groupAssignMenuItemActionPerformed(ActionEvent evt) {
        log.debug("groupAssignMenuItem.actionPerformed, event=" + evt);

        doGroup();
    }

    private List<PipelineDefinition> getSelectedTriggers() {
        List<PipelineDefinition> selectedTriggers = new LinkedList<>();

        int[] selectedRows = triggersOutline.getSelectedRows();

        for (int selectedRow : selectedRows) {
            if (selectedRow >= 0) {
                int modelIndex = triggersOutline.convertRowIndexToModel(selectedRow);
                DefaultMutableTreeNode node = (DefaultMutableTreeNode) triggersOutlineModel
                    .getValueAt(modelIndex, 0);
                Object userObject = node.getUserObject();
                if (userObject instanceof PipelineDefinition) {
                    PipelineDefinition trigger = (PipelineDefinition) userObject;

                    selectedTriggers.add(trigger);
                }
            }
        }

        return selectedTriggers;
    }

    private void doGroup() {
        log.debug("doGroup() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        try {
            Group group = ZiggyGuiConsole.selectGroup();

            if (group != null) {
                List<PipelineDefinition> selectedTriggers = getSelectedTriggers();

                for (PipelineDefinition trigger : selectedTriggers) {
                    if (group == Group.DEFAULT_GROUP) {
                        trigger.setGroup(null);
                    } else {
                        trigger.setGroup(group);
                    }
                }

                PipelineDefinitionCrudProxy pipelineCrud = new PipelineDefinitionCrudProxy();
                pipelineCrud.saveChanges();

                triggersTreeModel.loadFromDatabase();
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doGroup() - end");
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(400, 300));
            this.add(getTableScrollPane(), BorderLayout.CENTER);
            this.add(getButtonPanel(), BorderLayout.NORTH);
        } catch (Exception e) {
            log.error("initGUI()", e);

            e.printStackTrace();
        }

        log.debug("initGUI() - end");
    }

    private JScrollPane getTableScrollPane() {
        log.debug("getTableScrollPane() - start");

        if (tableScrollPane == null) {
            tableScrollPane = new JScrollPane();
            tableScrollPane.setViewportView(getTriggersOutline());
        }

        log.debug("getTableScrollPane() - end");
        return tableScrollPane;
    }

    private JPanel getButtonPanel() {
        log.debug("getButtonPanel() - start");

        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(20);
            buttonPanelLayout.setAlignment(FlowLayout.LEFT);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getNewButton());
            buttonPanel.add(getFireButton());
            buttonPanel.add(getRefreshButton());
            buttonPanel.add(getExpandAllButton());
            buttonPanel.add(getCollapseAllButton());
        }

        log.debug("getButtonPanel() - end");
        return buttonPanel;
    }

    private Outline getTriggersOutline() {
        if (triggersOutline == null) {
            triggersTreeModel = new TriggersTreeModel();
            TriggersRowModel triggersRowModel = new TriggersRowModel(triggersTreeModel);
            triggersOutlineModel = DefaultOutlineModel.createOutlineModel(triggersTreeModel,
                triggersRowModel, false, "Trigger Name");

            triggersOutline = new Outline();
            // triggersOutline.setRootVisible(false);
            triggersOutline.setModel(triggersOutlineModel);
            triggersTreeModel.setTriggersOutline(triggersOutline);
            // triggersOutline.setRenderDataProvider(new RenderData());
            DefaultMutableTreeNode defaultGroupNode = triggersTreeModel.getDefaultGroupNode();
            if (defaultGroupNode != null) {
                triggersOutline.expandPath(new TreePath(defaultGroupNode.getPath()));
            }

            setComponentPopupMenu(triggersOutline, getTriggerTablePopupMenu());
            triggersOutline.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent evt) {
                    log.debug("mouseClicked(MouseEvent) - start");

                    triggerTableMouseClicked(evt);

                    log.debug("mouseClicked(MouseEvent) - end");
                }
            });
        }
        return triggersOutline;
    }

    private JButton getNewButton() {
        log.debug("getNewButton() - start");

        if (newButton == null) {
            newButton = new JButton();
            newButton.setText("New");
            newButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                newButtonActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getNewButton() - end");
        return newButton;
    }

    private JButton getFireButton() {
        log.debug("getFireButton() - start");

        if (fireButton == null) {
            fireButton = new JButton();
            fireButton.setText("Fire");
            fireButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                fireButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getFireButton() - end");
        return fireButton;
    }

    private JPopupMenu getTriggerTablePopupMenu() {
        if (triggerTablePopupMenu == null) {
            triggerTablePopupMenu = new JPopupMenu();
            triggerTablePopupMenu.add(getEditMenuItem());
            triggerTablePopupMenu.add(getDeleteMenuItem());
            triggerTablePopupMenu.add(getCloneMenuItem());
            triggerTablePopupMenu.add(getGroupAssignMenuItem());
        }
        return triggerTablePopupMenu;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
                ZTable table = (ZTable) parent;
                int selectedTableRow = table.rowAtPoint(e.getPoint());
                // windows bug? works ok on Linux/gtk. Here's a workaround:
                if (selectedTableRow == -1) {
                    selectedTableRow = table.getSelectedRow();
                }
                selectedModelRow = table.convertRowIndexToModel(selectedTableRow);

                log.debug("selectedTableRow = " + selectedTableRow);
                log.debug("selectedModelRow = " + selectedModelRow);
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }
        });
    }

    private JMenuItem getEditMenuItem() {
        if (editMenuItem == null) {
            editMenuItem = new JMenuItem();
            editMenuItem.setText("Edit Trigger Parameters...");
            editMenuItem.addActionListener(this::editMenuItemActionPerformed);
        }
        return editMenuItem;
    }

    private JMenuItem getDeleteMenuItem() {
        if (deleteMenuItem == null) {
            deleteMenuItem = new JMenuItem();
            deleteMenuItem.setText("Delete Trigger...");
            deleteMenuItem.addActionListener(this::deleteMenuItemActionPerformed);
        }
        return deleteMenuItem;
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("Refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }

    private JMenuItem getCloneMenuItem() {
        if (cloneMenuItem == null) {
            cloneMenuItem = new JMenuItem();
            cloneMenuItem.setText("Clone Trigger...");
            cloneMenuItem.addActionListener(this::cloneMenuItemActionPerformed);
        }
        return cloneMenuItem;
    }

    private JMenuItem getGroupAssignMenuItem() {
        if (groupAssignMenuItem == null) {
            groupAssignMenuItem = new JMenuItem();
            groupAssignMenuItem.setText("Assign Group...");
            groupAssignMenuItem.addActionListener(this::groupAssignMenuItemActionPerformed);
        }
        return groupAssignMenuItem;
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        log.debug("main(String[]) - start");

        JFrame frame = new JFrame();
        frame.getContentPane().add(new OpsTriggersPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);

        log.debug("main(String[]) - end");
    }

    private JButton getExpandAllButton() {
        if (expandAllButton == null) {
            expandAllButton = new JButton();
            expandAllButton.setText("+");
            expandAllButton.addActionListener(this::expandAllButtonActionPerformed);
        }
        return expandAllButton;
    }

    private JButton getCollapseAllButton() {
        if (collapseAllButton == null) {
            collapseAllButton = new JButton();
            collapseAllButton.setText("-");
            collapseAllButton.addActionListener(this::collapseAllButtonActionPerformed);
        }
        return collapseAllButton;
    }
}

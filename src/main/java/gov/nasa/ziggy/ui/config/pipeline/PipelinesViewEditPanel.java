package gov.nasa.ziggy.ui.config.pipeline;

import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.AbstractClonableViewEditPanel;
import gov.nasa.ziggy.ui.config.group.GroupSelectorDialog;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelinesViewEditPanel extends AbstractClonableViewEditPanel {
    private static final Logger log = LoggerFactory.getLogger(PipelinesViewEditPanel.class);

    private PipelinesTableModel pipelinesTableModel; // do NOT init to null! (see getTableModel)
    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    private JMenuItem versionMenuItem;

    private JMenuItem groupMenuItem;

    public PipelinesViewEditPanel() throws PipelineUIException {
        super(true, true);

        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();

        initGUI();

        JPopupMenu menu = getPopupMenu();
        menu.add(getVersionMenuItem());
        menu.add(getGroupMenuItem());
    }

    @Override
    protected void doNew() {
        log.debug("doNew() - start");

        try {
            String newPipelineName = ZiggyGuiConsole.showInputDialog(
                "Enter the name for the new Pipeline Definition", "New Pipeline Definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newPipelineName == null || newPipelineName.length() == 0) {
                MessageUtil.showError(this, "Please enter a pipeline name");
                return;
            }

            showEditDialog(new PipelineDefinition(newPipelineName));

            pipelinesTableModel.loadFromDatabase();

            ZiggyGuiConsole.reloadConfigTreeModel();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doNew() - end");
    }

    @Override
    protected void doClone(int row) {
        log.debug("doClone() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        PipelineDefinition selectedPipeline = pipelinesTableModel.getPipelineAtRow(row);

        try {
            String newPipelineName = ZiggyGuiConsole.showInputDialog(
                "Enter the name for the new Pipeline Definition", "New Pipeline Definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newPipelineName == null || newPipelineName.length() == 0) {
                MessageUtil.showError(this, "Please enter a pipeline name");
                return;
            }

            PipelineDefinition newPipelineDefinition = new PipelineDefinition(selectedPipeline);
            newPipelineDefinition.rename(newPipelineName);

            showEditDialog(newPipelineDefinition);

            pipelinesTableModel.loadFromDatabase();

            ZiggyGuiConsole.reloadConfigTreeModel();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doClone() - end");
    }

    @Override
    protected void doRename(int row) {
        log.debug("doRename() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        PipelineDefinition selectedPipeline = pipelinesTableModel.getPipelineAtRow(row);

        try {
            String newPipelineName = ZiggyGuiConsole.showInputDialog(
                "Enter the new name for this Pipeline Definition", "Rename Pipeline Definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newPipelineName == null || newPipelineName.length() == 0) {
                MessageUtil.showError(this, "Please enter a pipeline name");
                return;
            }

            pipelineDefinitionCrud.rename(selectedPipeline, newPipelineName);
            pipelinesTableModel.loadFromDatabase();

            ZiggyGuiConsole.reloadConfigTreeModel();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doRename() - end");
    }

    private void doVersion(int row) {
        log.debug("doVersion() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        PipelineDefinition selectedPipeline = pipelinesTableModel.getPipelineAtRow(row);

        try {
            PipelineDefinition newPipelineDefinition = selectedPipeline.newVersion();

            PipelineDefinitionCrudProxy pipelineDefCrud = new PipelineDefinitionCrudProxy();
            pipelineDefCrud.save(newPipelineDefinition);

            pipelinesTableModel.loadFromDatabase();

            ZiggyGuiConsole.reloadConfigTreeModel();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doVersion() - end");
    }

    private void doGroup(int row) {
        log.debug("doGroup() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        try {
            PipelineDefinition selectedPipeline = pipelinesTableModel.getPipelineAtRow(row);
            Group selectedGroup = GroupSelectorDialog.selectGroup();

            if (selectedGroup != null) {
                selectedPipeline.setGroup(selectedGroup);
                PipelineDefinitionCrudProxy pipelineDefCrud = new PipelineDefinitionCrudProxy();
                pipelineDefCrud.saveChanges(selectedPipeline);
            }

            pipelinesTableModel.loadFromDatabase();

            ZiggyGuiConsole.reloadConfigTreeModel();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doGroup() - end");
    }

    @Override
    protected void doEdit(int row) {
        log.debug("doEdit(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        showEditDialog(pipelinesTableModel.getPipelineAtRow(row));

        pipelinesTableModel.loadFromDatabase();

        log.debug("doEdit(int) - end");
    }

    @Override
    protected void doDelete(int row) {
        log.debug("doDelete(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        PipelineDefinition pipeline = pipelinesTableModel.getPipelineAtRow(row);

        if (!pipeline.isLocked()) {
            int choice = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete pipeline '" + pipeline.getName() + "'?");

            if (choice == JOptionPane.YES_OPTION) {
                try {
                    pipelineDefinitionCrud.deletePipeline(pipeline);
                    pipelinesTableModel.loadFromDatabase();
                    ZiggyGuiConsole.reloadConfigTreeModel();
                } catch (Exception e) {
                    MessageUtil.showError(this, e);
                }
            }
        } else {
            JOptionPane.showMessageDialog(this,
                "Can't delete a locked pipeline definition.  Pipeline definitions are locked when referenced by a pipeline instance",
                "Error", JOptionPane.ERROR_MESSAGE);
        }

        log.debug("doDelete(int) - end");
    }

    @Override
    protected void doRefresh() {
        try {
            pipelinesTableModel.loadFromDatabase();
            ZiggyGuiConsole.reloadConfigTreeModel();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(PipelineDefinition pipeline) {
        log.debug("showEditDialog() - start");

        try {
            PipelineEditDialog inst = ZiggyGuiConsole.newPipelineEditDialog(pipeline);

            inst.setVisible(true);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("showEditDialog() - end");
    }

    @Override
    protected AbstractTableModel getTableModel() throws PipelineUIException {
        log.debug("getTableModel() - start");

        if (pipelinesTableModel == null) {
            pipelinesTableModel = new PipelinesTableModel();
            pipelinesTableModel.register();
        }

        log.debug("getTableModel() - end");
        return pipelinesTableModel;
    }

    private JMenuItem getVersionMenuItem() {
        log.debug("getversionMenuItem() - start");

        if (versionMenuItem == null) {
            versionMenuItem = new JMenuItem();
            versionMenuItem.setText("New version of selected pipeline (unlock)...");
            versionMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                versionMenuItemActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getVersionMenuItem() - end");
        return versionMenuItem;
    }

    private void versionMenuItemActionPerformed() {
        doVersion(selectedModelRow);
    }

    private JMenuItem getGroupMenuItem() {
        log.debug("getGroupMenuItem() - start");

        if (groupMenuItem == null) {
            groupMenuItem = new JMenuItem();
            groupMenuItem.setText("Set Group...");
            groupMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                groupMenuItemActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getGroupMenuItem() - end");
        return groupMenuItem;
    }

    private void groupMenuItemActionPerformed() {
        doGroup(selectedModelRow);
    }

    @Override
    protected String getEditMenuText() {
        return "Edit selected Pipeline...";
    }

    @Override
    protected String getNewMenuText() {
        return "New Pipeline...";
    }

    @Override
    protected String getDeleteMenuText() {
        return "Delete selected Pipeline...";
    }

    @Override
    protected String getCloneMenuText() {
        return "Clone selected Pipeline...";
    }

    @Override
    protected String getRenameMenuText() {
        return "Rename selected pipeline...";
    }
}

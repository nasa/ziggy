package gov.nasa.ziggy.ui.config.module;

import javax.swing.JOptionPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.AbstractClonableViewEditPanel;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.PipelineModuleDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ModuleLibraryViewEditPanel extends AbstractClonableViewEditPanel {
    public static final Logger log = LoggerFactory.getLogger(ModuleLibraryViewEditPanel.class);

    private ModuleLibraryTableModel moduleLibraryTableModel; // do NOT init to null! (see
    // getTableModel)

    private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud;

    public ModuleLibraryViewEditPanel() throws PipelineUIException {
        super(true, true);

        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();

        initGUI();
    }

    @Override
    protected AbstractTableModel getTableModel() throws PipelineUIException {
        log.debug("getTableModel() - start");

        if (moduleLibraryTableModel == null) {
            moduleLibraryTableModel = new ModuleLibraryTableModel();
            moduleLibraryTableModel.register();
        }

        log.debug("getTableModel() - end");
        return moduleLibraryTableModel;
    }

    @Override
    protected void doNew() {
        log.debug("doNew() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        String newModuleName = ZiggyGuiConsole.showInputDialog(
            "Enter the name for the new Module Definition", "New Pipeline Module Definition",
            JOptionPane.PLAIN_MESSAGE);

        if (newModuleName == null || newModuleName.length() == 0) {
            MessageUtil.showError(this, "Please enter a module name");
            return;
        }

        showEditDialog(new PipelineModuleDefinition(newModuleName));

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

        PipelineModuleDefinition selectedModule = moduleLibraryTableModel.getModuleAtRow(row);

        try {
            String newModuleName = ZiggyGuiConsole.showInputDialog(
                "Enter the name for the new Module Definition", "New Module Definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newModuleName == null || newModuleName.length() == 0) {
                MessageUtil.showError(this, "Please enter a module name");
                return;
            }

            PipelineModuleDefinition newModuleDefinition = new PipelineModuleDefinition(
                selectedModule);
            newModuleDefinition.rename(newModuleName);

            showEditDialog(newModuleDefinition);

            moduleLibraryTableModel.loadFromDatabase();
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

        PipelineModuleDefinition selectedModule = moduleLibraryTableModel.getModuleAtRow(row);

        try {
            String newModuleName = ZiggyGuiConsole.showInputDialog(
                "Enter the new name for this Module Definition", "Rename Module Definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newModuleName == null || newModuleName.length() == 0) {
                MessageUtil.showError(this, "Please enter a module name");
                return;
            }

            pipelineModuleDefinitionCrud.rename(selectedModule, newModuleName);
            moduleLibraryTableModel.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("doRename() - end");
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

        showEditDialog(moduleLibraryTableModel.getModuleAtRow(row));

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

        PipelineModuleDefinition module = moduleLibraryTableModel.getModuleAtRow(row);

        if (!module.isLocked()) {
            int choice = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete module '" + module.getName() + "'?");

            if (choice == JOptionPane.YES_OPTION) {
                try {
                    pipelineModuleDefinitionCrud.delete(module);
                    moduleLibraryTableModel.loadFromDatabase();
                } catch (Throwable e) {
                    MessageUtil.showError(this, e);
                }
            }
        } else {
            JOptionPane.showMessageDialog(this,
                "Can't delete a locked module definition.  Modules are locked when referenced by a pipeline instance",
                "Error", JOptionPane.ERROR_MESSAGE);
        }

        log.debug("doDelete(int) - end");
    }

    @Override
    protected void doRefresh() {
        try {
            moduleLibraryTableModel.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(PipelineModuleDefinition module) {
        log.debug("showEditDialog() - start");

        ModuleEditDialog inst = ZiggyGuiConsole.newModuleEditDialog(module);

        inst.setVisible(true);

        if (!inst.isCancelled()) {
            try {
                moduleLibraryTableModel.loadFromDatabase();
            } catch (Exception e) {
                log.error("showEditDialog(User)", e);

                MessageUtil.showError(this, e);
            }
        }

        log.debug("showEditDialog() - end");
    }

    @Override
    protected String getEditMenuText() {
        return "Edit selected module...";
    }

    @Override
    protected String getNewMenuText() {
        return "Add module...";
    }

    @Override
    protected String getDeleteMenuText() {
        return "Delete selected module...";
    }

    @Override
    protected String getCloneMenuText() {
        return "Clone selected module...";
    }

    @Override
    protected String getRenameMenuText() {
        return "Rename selected module...";
    }
}

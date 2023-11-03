package gov.nasa.ziggy.ui.module;

import java.awt.Cursor;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.CrudProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineModuleDefinitionCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ViewEditModuleLibraryPanel extends AbstractViewEditPanel<PipelineModuleDefinition> {

    private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();

    public ViewEditModuleLibraryPanel() {
        super(new ModuleLibraryTableModel());

        buildComponent();
    }

    @Override
    protected Set<OptionalViewEditFunctions> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunctions.DELETE, OptionalViewEditFunctions.NEW,
            OptionalViewEditFunctions.RENAME);
    }

    @Override
    protected void create() {
        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        String newModuleName = JOptionPane.showInputDialog(SwingUtilities.getWindowAncestor(this),
            "Enter the name for the new Module Definition", "New Pipeline Module Definition",
            JOptionPane.PLAIN_MESSAGE);

        if (newModuleName == null) {
            return;
        }
        if (newModuleName.isEmpty()) {
            MessageUtil.showError(this, "Please enter a module name");
            return;
        }

        // TODO Fix this wait cursor so it actually shows
        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        showEditDialog(new PipelineModuleDefinition(newModuleName));
        setCursor(null);
    }

    @Override
    protected void rename(int row) {
        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        PipelineModuleDefinition selectedModule = ziggyTable.getContentAtViewRow(row);

        try {
            String newModuleName = JOptionPane.showInputDialog(
                SwingUtilities.getWindowAncestor(this),
                "Enter the new name for this Module Definition", "Rename Module Definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newModuleName == null) {
                return;
            }
            if (newModuleName.isEmpty()) {
                MessageUtil.showError(this, "Please enter a module name");
                return;
            }

            pipelineModuleDefinitionCrud.rename(selectedModule, newModuleName);
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void edit(int row) {
        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        showEditDialog(ziggyTable.getContentAtViewRow(row));
        setCursor(null);
    }

    @Override
    protected void delete(int row) {

        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        PipelineModuleDefinition module = ziggyTable.getContentAtViewRow(row);

        if (!module.isLocked()) {
            int choice = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete module '" + module.getName() + "'?");

            if (choice == JOptionPane.YES_OPTION) {
                try {
                    pipelineModuleDefinitionCrud.delete(module);
                    ziggyTable.loadFromDatabase();
                } catch (Throwable e) {
                    MessageUtil.showError(this, e);
                }
            }
        } else {
            JOptionPane.showMessageDialog(this,
                "Can't delete a locked module definition. Modules are locked when referenced by a pipeline instance",
                "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(PipelineModuleDefinition module) {
        EditModuleDialog dialog = new EditModuleDialog(SwingUtilities.getWindowAncestor(this),
            module);
        dialog.setVisible(true);

        if (!dialog.isCancelled()) {
            try {
                ziggyTable.loadFromDatabase();
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }
    }

    private static class ModuleLibraryTableModel
        extends AbstractDatabaseModel<PipelineModuleDefinition> {
        private List<PipelineModuleDefinition> modules = new LinkedList<>();
        private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();

        private static final String[] COLUMN_NAMES = { "ID", "Name", "Version", "Locked", "User",
            "Modified" };
        private static final Class<?>[] COLUMN_CLASSES = { Integer.class, String.class,
            Integer.class, Boolean.class, String.class, Object.class };

        @Override
        public void loadFromDatabase() {
            try {
                modules = pipelineModuleDefinitionCrud.retrieveLatestVersions();
            } catch (ConsoleSecurityException ignore) {
            }

            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            validityCheck();
            return modules.size();
        }

        @Override
        public int getColumnCount() {
            validityCheck();
            return 6;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            validityCheck();

            PipelineModuleDefinition module = modules.get(rowIndex);

            AuditInfo auditInfo = module.getAuditInfo();

            User lastChangedUser = null;
            Date lastChangedTime = null;

            if (auditInfo != null) {
                lastChangedUser = auditInfo.getLastChangedUser();
                lastChangedTime = auditInfo.getLastChangedTime();
            }

            return switch (columnIndex) {
                case 0 -> module.getId();
                case 1 -> module.getName();
                case 2 -> module.getVersion();
                case 3 -> module.isLocked();
                case 4 -> lastChangedUser != null ? lastChangedUser.getLoginName() : "---";
                case 5 -> lastChangedTime != null ? lastChangedTime : "---";
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            validityCheck();
            return COLUMN_CLASSES[columnIndex];
        }

        @Override
        public String getColumnName(int column) {
            validityCheck();
            return COLUMN_NAMES[column];
        }

        @Override
        public PipelineModuleDefinition getContentAtRow(int row) {
            validityCheck();
            return modules.get(row);
        }

        @Override
        public Class<PipelineModuleDefinition> tableModelContentClass() {
            return PipelineModuleDefinition.class;
        }
    }
}

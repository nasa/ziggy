package gov.nasa.ziggy.ui.module;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * View / Edit panel for {@link PipelineModuleDefinition} instances.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ViewEditModuleLibraryPanel extends AbstractViewEditPanel<PipelineModuleDefinition> {

    private static final Logger log = LoggerFactory.getLogger(ViewEditModuleLibraryPanel.class);

    private final PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();

    public ViewEditModuleLibraryPanel() {
        super(new ModuleLibraryTableModel());

        buildComponent();

        ZiggyMessenger.subscribe(InvalidateConsoleModelsMessage.class, this::invalidateModel);
    }

    private void invalidateModel(InvalidateConsoleModelsMessage message) {
        ziggyTable.loadFromDatabase();
    }

    @Override
    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunction.DELETE, OptionalViewEditFunction.NEW,
            OptionalViewEditFunction.RENAME);
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected void create() {

        String newModuleName = readModuleName("Enter the name for the new module definition",
            "New pipeline module definition");
        if (newModuleName == null) {
            return;
        }

        showEditDialog(new PipelineModuleDefinition(newModuleName));
    }

    @Override
    protected void edit(int row) {
        showEditDialog(ziggyTable.getContentAtViewRow(row));
    }

    @Override
    protected void rename(int row) {

        String newModuleName = readModuleName("Enter the new name for this module definition",
            "Rename module definition");
        if (newModuleName == null) {
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                pipelineModuleDefinitionOperations().rename(ziggyTable.getContentAtViewRow(row),
                    newModuleName);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    ziggyTable.loadFromDatabase();
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(ViewEditModuleLibraryPanel.this, e);
                }
            }
        }.execute();
    }

    @Override
    protected void delete(int row) {

        PipelineModuleDefinition module = ziggyTable.getContentAtViewRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete module '" + module.getName() + "'?");
        if (choice != JOptionPane.YES_OPTION) {
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                pipelineModuleDefinitionOperations().delete(module);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    ziggyTable.loadFromDatabase();
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(ViewEditModuleLibraryPanel.this, e);
                }

                super.done();
            }
        }.execute();
    }

    private String readModuleName(String message, String title) {
        while (true) {
            String moduleName = JOptionPane.showInputDialog(SwingUtilities.getWindowAncestor(this),
                message, title, JOptionPane.PLAIN_MESSAGE);
            if (moduleName == null) {
                return null;
            }

            if (moduleName.isBlank()) {
                MessageUtils.showError(this, "Please enter a module name");
            } else if (pipelineModuleDefinitionOperations()
                .pipelineModuleDefinition(moduleName) != null) {
                MessageUtils.showError(this,
                    moduleName + " already exists; please enter a unique module name");
            } else {
                return moduleName;
            }
        }
    }

    private void showEditDialog(PipelineModuleDefinition module) {
        EditModuleDialog dialog = new EditModuleDialog(SwingUtilities.getWindowAncestor(this),
            module);
        dialog.setVisible(true);

        if (dialog.isCancelled()) {
            return;
        }

        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtils.showError(this, e);
        }
    }

    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }

    private static class ModuleLibraryTableModel
        extends AbstractZiggyTableModel<PipelineModuleDefinition> implements DatabaseModel {

        private static final String[] COLUMN_NAMES = { "ID", "Name", "Version", "Locked", "User",
            "Modified" };
        private static final Class<?>[] COLUMN_CLASSES = { Integer.class, String.class,
            Integer.class, Boolean.class, String.class, Object.class };

        private List<PipelineModuleDefinition> modules = new LinkedList<>();

        private final PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();

        @Override
        public void loadFromDatabase() {
            new SwingWorker<List<PipelineModuleDefinition>, Void>() {
                @Override
                protected List<PipelineModuleDefinition> doInBackground() throws Exception {
                    return pipelineModuleDefinitionOperations().pipelineModuleDefinitions();
                }

                @Override
                protected void done() {
                    try {
                        modules = get();
                        fireTableDataChanged();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Could not load pipeline module definitions", e);
                    }
                }
            }.execute();
        }

        @Override
        public int getRowCount() {
            return modules.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            PipelineModuleDefinition module = modules.get(rowIndex);

            AuditInfo auditInfo = module.getAuditInfo();

            String lastChangedUser = null;
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
                case 4 -> lastChangedUser != null ? lastChangedUser : "---";
                case 5 -> lastChangedTime != null ? lastChangedTime : "---";
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return COLUMN_CLASSES[columnIndex];
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public PipelineModuleDefinition getContentAtRow(int row) {
            return modules.get(row);
        }

        @Override
        public Class<PipelineModuleDefinition> tableModelContentClass() {
            return PipelineModuleDefinition.class;
        }

        private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
            return pipelineModuleDefinitionOperations;
        }
    }
}

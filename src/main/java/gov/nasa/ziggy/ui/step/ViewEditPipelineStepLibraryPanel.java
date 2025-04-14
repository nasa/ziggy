package gov.nasa.ziggy.ui.step;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * View / Edit panel for {@link PipelineStep} instances.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ViewEditPipelineStepLibraryPanel extends AbstractViewEditPanel<PipelineStep> {

    private static final Logger log = LoggerFactory
        .getLogger(ViewEditPipelineStepLibraryPanel.class);

    public ViewEditPipelineStepLibraryPanel() {
        super(new StepLibraryTableModel());
        ZiggyMessenger.subscribe(PipelineInstanceStartedMessage.class, this::invalidateModel);
    }

    private void invalidateModel(PipelineInstanceStartedMessage message) {
        ziggyTable.loadFromDatabase();
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
    protected void edit(int row) {
        showEditDialog(ziggyTable.getContentAtViewRow(row));
    }

    private void showEditDialog(PipelineStep pipelineStep) {
        EditPipelineStepDialog dialog = new EditPipelineStepDialog(
            SwingUtilities.getWindowAncestor(this), pipelineStep);
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

    private static class StepLibraryTableModel extends AbstractZiggyTableModel<PipelineStep>
        implements DatabaseModel {

        private static final String[] COLUMN_NAMES = { "ID", "Name", "Version", "Locked", "User",
            "Modified" };
        private static final Class<?>[] COLUMN_CLASSES = { Integer.class, String.class,
            Integer.class, Boolean.class, String.class, Object.class };

        private List<PipelineStep> pipelineSteps = new LinkedList<>();

        private final PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();

        @Override
        public void loadFromDatabase() {
            new SwingWorker<List<PipelineStep>, Void>() {
                @Override
                protected List<PipelineStep> doInBackground() throws Exception {
                    return pipelineStepOperations().pipelineSteps();
                }

                @Override
                protected void done() {
                    try {
                        pipelineSteps = get();
                        fireTableDataChanged();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Could not load pipeline steps", e);
                    }
                }
            }.execute();
        }

        @Override
        public int getRowCount() {
            return pipelineSteps.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            PipelineStep pipelineStep = pipelineSteps.get(rowIndex);

            AuditInfo auditInfo = pipelineStep.getAuditInfo();

            String lastChangedUser = null;
            Date lastChangedTime = null;

            if (auditInfo != null) {
                lastChangedUser = auditInfo.getLastChangedUser();
                lastChangedTime = auditInfo.getLastChangedTime();
            }

            return switch (columnIndex) {
                case 0 -> pipelineStep.getId();
                case 1 -> pipelineStep.getName();
                case 2 -> pipelineStep.getVersion();
                case 3 -> pipelineStep.isLocked();
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
        public PipelineStep getContentAtRow(int row) {
            return pipelineSteps.get(row);
        }

        @Override
        public Class<PipelineStep> tableModelContentClass() {
            return PipelineStep.class;
        }

        private PipelineStepOperations pipelineStepOperations() {
            return pipelineStepOperations;
        }
    }
}

package gov.nasa.ziggy.ui.pipeline;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.START;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dialog;
import java.awt.event.ActionEvent;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.services.messages.InvalidateConsoleModelsMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditGroupPanel;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * Panel for viewing and editing pipelines.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ViewEditPipelinesPanel extends AbstractViewEditGroupPanel<PipelineDefinition> {

    private static final long serialVersionUID = 20240614L;

    private final PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();

    public ViewEditPipelinesPanel(PipelineRowModel rowModel,
        ZiggyTreeModel<PipelineDefinition> treeModel) {
        super(rowModel, treeModel, "Name");
        buildComponent();

        ZiggyMessenger.subscribe(InvalidateConsoleModelsMessage.class, this::invalidateModel);
    }

    /**
     * Convenience method that can be used instead of the constructor. Helpful because the row model
     * for parameter sets needs the tree model in its constructor.
     */
    public static ViewEditPipelinesPanel newInstance() {
        ZiggyTreeModel<PipelineDefinition> treeModel = new ZiggyTreeModel<>(
            PipelineDefinition.class,
            () -> new PipelineDefinitionOperations().allPipelineDefinitions());
        return new ViewEditPipelinesPanel(new PipelineRowModel(), treeModel);
    }

    private void invalidateModel(InvalidateConsoleModelsMessage message) {
        ziggyTable.loadFromDatabase();
    }

    @Override
    protected List<JButton> buttons() {
        List<JButton> buttons = super.buttons();
        buttons.add(createButton(START, this::start));
        return buttons;
    }

    private void start(ActionEvent evt) {

        int tableRow = ziggyTable.getSelectedRow();
        selectedModelRow = ziggyTable.convertRowIndexToModel(tableRow);
        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(selectedModelRow);

        // TODO Delete once the Start button is disabled if a non-pipeline row is selected
        if (pipeline == null) {
            return;
        }

        try {
            new StartPipelineDialog(SwingUtilities.getWindowAncestor(this), pipeline)
                .setVisible(true);
        } catch (Throwable e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    @Override
    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return Set.of(
            /* TODO Implement OptionalViewEditFunctions.NEW, per ZIGGY-284 */ OptionalViewEditFunction.VIEW,
            OptionalViewEditFunction.COPY, OptionalViewEditFunction.RENAME,
            OptionalViewEditFunction.DELETE);
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected void create() {

        NewPipelineDialog newPipelineDialog = new NewPipelineDialog(
            SwingUtilities.getWindowAncestor(this));
        newPipelineDialog.setVisible(true);
        if (newPipelineDialog.isCancelled()) {
            return;
        }

        new EditPipelineDialog(SwingUtilities.getWindowAncestor(this),
            new PipelineDefinition(newPipelineDialog.getPipelineName())).setVisible(true);

        try {
            ziggyTable.loadFromDatabase();
        } catch (PipelineException e) {
            MessageUtils.showError(this, e);
        }
    }

    @Override
    protected void view(int row) {

        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(row);
        JDialog dialog = new JDialog(SwingUtilities.getWindowAncestor(this), pipeline.getName(),
            JDialog.DEFAULT_MODALITY_TYPE);
        dialog.setLocationRelativeTo(SwingUtilities.getWindowAncestor(this));
        dialog.add(new PipelineGraphCanvas(pipeline), BorderLayout.CENTER);
        dialog.add(ZiggySwingUtils.createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);
        dialog.pack();
        dialog.setVisible(true);
    }

    private void close(ActionEvent evt) {
        Dialog dialog = ZiggySwingUtils.getDialog((Component) evt.getSource());
        if (dialog != null) {
            dialog.setVisible(false);
        }
    }

    @Override
    protected void edit(int row) {

        EditPipelineDialog dialog = new EditPipelineDialog(SwingUtilities.getWindowAncestor(this),
            ziggyTable.getContentAtViewRow(row));
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

    @Override
    protected void copy(int row) {

        String newPipelineName = readPipelineName("Enter the name for the new pipeline definition",
            "New pipeline definition");
        if (newPipelineName == null) {
            return;
        }

        PipelineDefinition newPipeline = ziggyTable.getContentAtViewRow(row).newInstance();
        newPipeline.setName(newPipelineName);

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                pipelineDefinitionOperations().merge(newPipeline);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    ziggyTable.loadFromDatabase();
                } catch (Exception e) {
                    MessageUtils.showError(ViewEditPipelinesPanel.this, e);
                }
            }
        }.execute();
    }

    @Override
    protected void rename(int row) {

        String newPipelineName = readPipelineName("Enter the new name for this pipeline definition",
            "Rename pipeline definition");
        if (newPipelineName == null) {
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                pipelineDefinitionOperations().rename(ziggyTable.getContentAtViewRow(row),
                    newPipelineName);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    ziggyTable.loadFromDatabase();
                } catch (Exception e) {
                    MessageUtils.showError(ViewEditPipelinesPanel.this, e);
                }
            }
        }.execute();
    }

    @Override
    protected void delete(int row) {

        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete pipeline " + pipeline.getName() + "?");
        if (choice != JOptionPane.YES_OPTION) {
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                pipelineDefinitionOperations().delete(pipeline);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    ziggyTable.loadFromDatabase();
                } catch (Exception e) {
                    MessageUtils.showError(ViewEditPipelinesPanel.this, e);
                }
            }
        }.execute();
    }

    private String readPipelineName(String message, String title) {
        while (true) {
            String pipelineName = JOptionPane.showInputDialog(
                SwingUtilities.getWindowAncestor(this), message, title, JOptionPane.PLAIN_MESSAGE);
            if (pipelineName == null) {
                return null;
            }
            if (pipelineName.isBlank()) {
                MessageUtils.showError(this, "Please enter a pipeline name");
            } else if (pipelineDefinitionOperations().pipelineDefinition(pipelineName) != null) {
                MessageUtils.showError(this,
                    pipelineName + " already exists; please enter a unique pipeline name");
            } else {
                return pipelineName;
            }
        }
    }

    private PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    private static class PipelineRowModel
        implements RowModel, ModelContentClass<PipelineDefinition> {

        private static final String[] COLUMN_NAMES = { "Version", "Locked", "User", "Modified",
            "Node count" };
        private static final Class<?>[] COLUMN_CLASSES = { Integer.class, Boolean.class,
            String.class, Object.class, Integer.class };

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        private void checkColumnArgument(int columnIndex) {
            checkArgument(columnIndex >= 0 && columnIndex < COLUMN_NAMES.length, "column value of "
                + columnIndex + " outside of expected range from 0 to " + COLUMN_NAMES.length);
        }

        @Override
        public boolean isCellEditable(Object node, int column) {
            return false;
        }

        @Override
        public void setValueFor(Object node, int column, Object value) {
            // not editable
        }

        @Override
        public Object getValueFor(Object treeNode, int columnIndex) {
            checkColumnArgument(columnIndex);
            Object node = ((DefaultMutableTreeNode) treeNode).getUserObject();

            if (!(node instanceof PipelineDefinition)) {
                return null;
            }
            PipelineDefinition pipeline = (PipelineDefinition) node;

            AuditInfo auditInfo = pipeline.getAuditInfo();

            String lastChangedUser = null;
            Date lastChangedTime = null;

            if (auditInfo != null) {
                lastChangedUser = auditInfo.getLastChangedUser();
                lastChangedTime = auditInfo.getLastChangedTime();
            }

            return switch (columnIndex) {
                case 0 -> pipeline.getVersion();
                case 1 -> pipeline.isLocked();
                case 2 -> lastChangedUser != null ? lastChangedUser : "---";
                case 3 -> lastChangedTime != null ? lastChangedTime : "---";
                case 4 -> pipeline.getNodes().size();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            checkColumnArgument(columnIndex);
            return COLUMN_CLASSES[columnIndex];
        }

        @Override
        public String getColumnName(int column) {
            checkColumnArgument(column);
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<PipelineDefinition> tableModelContentClass() {
            return PipelineDefinition.class;
        }
    }
}

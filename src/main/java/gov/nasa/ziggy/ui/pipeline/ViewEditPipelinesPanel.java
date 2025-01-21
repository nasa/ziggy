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
import java.util.Map;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
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

    private static final long serialVersionUID = 20241002L;
    private static final String TYPE = "PipelineDefinition";

    private Action startAction;

    public ViewEditPipelinesPanel(PipelineRowModel rowModel,
        ZiggyTreeModel<PipelineDefinition> treeModel) {
        super(rowModel, treeModel, "Name");

        ziggyTable.getTable()
            .getSelectionModel()
            .setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

        ZiggyMessenger.subscribe(PipelineInstanceStartedMessage.class, this::invalidateModel);
    }

    /**
     * Convenience method that can be used instead of the constructor. Helpful because the row model
     * for parameter sets needs the tree model in its constructor.
     */
    public static ViewEditPipelinesPanel newInstance() {
        ZiggyTreeModel<PipelineDefinition> treeModel = new ZiggyTreeModel<>(TYPE,
            () -> new PipelineDefinitionOperations().allPipelineDefinitions());
        return new ViewEditPipelinesPanel(new PipelineRowModel(), treeModel);
    }

    private void invalidateModel(PipelineInstanceStartedMessage message) {
        ziggyTable.loadFromDatabase();
    }

    @SuppressWarnings("serial")
    @Override
    protected List<JButton> buttons() {
        List<JButton> buttons = super.buttons();
        startAction = new AbstractAction(START, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                start();
            }
        };
        buttons.add(createButton(startAction));
        return buttons;
    }

    private void start() {

        int tableRow = ziggyTable.getSelectedRow();
        selectedModelRow = ziggyTable.convertRowIndexToModel(tableRow);
        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(selectedModelRow);

        try {
            new StartPipelineDialog(SwingUtilities.getWindowAncestor(this), pipeline)
                .setVisible(true);
        } catch (Throwable e) {
            MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    @Override
    protected void updateActionState(Map<OptionalViewEditFunction, Action> actionByFunction) {
        startAction.setEnabled(ziggyTable.getTable().getSelectedRowCount() == 1);
        actionByFunction.get(OptionalViewEditFunction.EDIT)
            .setEnabled(ziggyTable.getTable().getSelectedRowCount() == 1);
        actionByFunction.get(OptionalViewEditFunction.VIEW)
            .setEnabled(ziggyTable.getTable().getSelectedRowCount() == 1);
    }

    @Override
    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunction.VIEW);
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
    protected String getType() {
        return TYPE;
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

package gov.nasa.ziggy.ui.pipeline;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.START;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dialog;
import java.awt.event.ActionEvent;
import java.util.Date;
import java.util.Set;

import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.tree.DefaultMutableTreeNode;

import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.TableModelContentClass;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.proxy.PipelineDefinitionCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.RetrieveLatestVersionsCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

/**
 * Panel for viewing and editing pipelines.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ViewEditPipelinesPanel extends AbstractViewEditPanel<PipelineDefinition> {

    private static final long serialVersionUID = 20230810L;

    private PipelineDefinitionCrudProxy crudProxy = new PipelineDefinitionCrudProxy();
    private ZiggyTreeModel<PipelineDefinition> treeModel;

    public ViewEditPipelinesPanel(PipelineRowModel rowModel,
        ZiggyTreeModel<PipelineDefinition> treeModel) {
        super(rowModel, treeModel, "Name");
        this.treeModel = treeModel;
        buildComponent();
    }

    /**
     * Convenience method that can be used instead of the constructor. Helpful because the row model
     * for parameter sets needs the tree model in its constructor.
     */
    public static ViewEditPipelinesPanel newInstance() {
        ZiggyTreeModel<PipelineDefinition> treeModel = new ZiggyTreeModel<>(
            new PipelineDefinitionCrudProxy());
        PipelineRowModel rowModel = new PipelineRowModel(treeModel);
        return new ViewEditPipelinesPanel(rowModel, treeModel);
    }

    @Override
    protected void buildComponent() {
        super.buildComponent();
        ZiggySwingUtils.addButtonsToPanel(getButtonPanel(), createButton(START, this::start));
        getPopupMenu().add(
            createMenuItem("New version of selected pipeline (unlock)" + DIALOG, this::newVersion),
            0);
    }

    private void start(ActionEvent evt) {
        checkPrivileges();

        int tableRow = ziggyTable.getSelectedRow();
        selectedModelRow = ziggyTable.convertRowIndexToModel(tableRow);
        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(selectedModelRow);
        if (pipeline != null) {
            try {
                new StartPipelineDialog(SwingUtilities.getWindowAncestor(this), pipeline)
                    .setVisible(true);
            } catch (Throwable e) {
                MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    private void newVersion(ActionEvent evt) {
        checkPrivileges();

        PipelineDefinition selectedPipeline = ziggyTable.getContentAtViewRow(selectedModelRow);

        try {
            // Make sure that an unlocked version of the selected pipeline is present in the
            // database
            PipelineDefinitionCrudProxy pipelineDefCrud = new PipelineDefinitionCrudProxy();
            pipelineDefCrud.createOrUpdate(selectedPipeline);

            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    @Override
    protected RetrieveLatestVersionsCrudProxy<PipelineDefinition> getCrudProxy() {
        return crudProxy;
    }

    @Override
    protected Set<OptionalViewEditFunctions> optionalViewEditFunctions() {
        return Set.of(
            /* TODO Implement OptionalViewEditFunctions.NEW, per ZIGGY-284 */ OptionalViewEditFunctions.VIEW,
            OptionalViewEditFunctions.GROUP, OptionalViewEditFunctions.COPY,
            OptionalViewEditFunctions.RENAME, OptionalViewEditFunctions.DELETE);
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void create() {
        checkPrivileges();

        NewPipelineDialog newPipelineDialog = new NewPipelineDialog(
            SwingUtilities.getWindowAncestor(this));
        newPipelineDialog.setVisible(true);
        if (newPipelineDialog.isCancelled()) {
            return;
        }

        EditPipelineDialog editDialog = new EditPipelineDialog(
            SwingUtilities.getWindowAncestor(this),
            new PipelineDefinition(newPipelineDialog.getPipelineName()), treeModel);
        editDialog.setVisible(true);

        try {
            ziggyTable.loadFromDatabase();
        } catch (PipelineException e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void view(int row) {
        checkPrivileges();

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
        checkPrivileges();

        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(row);
        if (pipeline == null) {
            return;
        }
        EditPipelineDialog editDialog = new EditPipelineDialog(
            SwingUtilities.getWindowAncestor(this), pipeline, treeModel);
        editDialog.setVisible(true);

        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void copy(int row) {
        checkPrivileges();

        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(row);
        if (pipeline == null) {
            return;
        }

        new PipelineDefinitionCrudProxy().createOrUpdate(pipeline.newInstance());

        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void rename(int row) {
        checkPrivileges();

        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(row);
        if (pipeline == null) {
            return;
        }

        try {
            String newPipelineName = JOptionPane.showInputDialog(
                SwingUtilities.getWindowAncestor(this),
                "Enter the new name for this pipeline definition", "Rename pipeline definition",
                JOptionPane.PLAIN_MESSAGE);

            if (newPipelineName == null) {
                return;
            }
            if (newPipelineName.isEmpty()) {
                MessageUtil.showError(this, "Please enter a pipeline name");
                return;
            }

            crudProxy.rename(pipeline, newPipelineName);
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    @Override
    protected void delete(int row) {
        checkPrivileges();

        PipelineDefinition pipeline = ziggyTable.getContentAtViewRow(row);
        if (pipeline == null) {
            return;
        }

        if (pipeline.isLocked()) {
            JOptionPane.showMessageDialog(this,
                "Can not delete locked pipeline definitions. "
                    + "Pipelines are locked when referenced by a pipeline instance",
                "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete pipeline " + pipeline.getName() + "?");
        if (choice != JOptionPane.YES_OPTION) {
            return;
        }

        try {
            crudProxy.deletePipeline(pipeline);
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private static class PipelineRowModel
        implements RowModel, TableModelContentClass<PipelineDefinition> {

        private static final String[] COLUMN_NAMES = { "Version", "Locked", "User", "Modified",
            "Node count" };
        private static final Class<?>[] COLUMN_CLASSES = { Integer.class, Boolean.class,
            String.class, Object.class, Integer.class };

        private ZiggyTreeModel<PipelineDefinition> treeModel;

        public PipelineRowModel(ZiggyTreeModel<PipelineDefinition> treeModel) {
            this.treeModel = treeModel;
        }

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
            treeModel.validityCheck();
            Object node = ((DefaultMutableTreeNode) treeNode).getUserObject();

            if (!(node instanceof PipelineDefinition)) {
                return null;
            }
            PipelineDefinition pipeline = (PipelineDefinition) node;

            AuditInfo auditInfo = pipeline.getAuditInfo();

            User lastChangedUser = null;
            Date lastChangedTime = null;

            if (auditInfo != null) {
                lastChangedUser = auditInfo.getLastChangedUser();
                lastChangedTime = auditInfo.getLastChangedTime();
            }

            return switch (columnIndex) {
                case 0 -> pipeline.getVersion();
                case 1 -> pipeline.isLocked();
                case 2 -> lastChangedUser != null ? lastChangedUser.getLoginName() : "---";
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

package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createPopupMenu;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.GroupLayout;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.ListSelectionModel;

import org.netbeans.swing.etable.ETable;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.services.messages.WorkerResources;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.TableMouseListener;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Displays the max workers and Java heap size values for each node, and allows the user to edit the
 * values. The value editing is actually performed by the
 * {@link PipelineDefinitionNodeResourcesDialog}, which is launched from this dialog box.
 *
 * @author PT
 * @author Bill Wohler
 */
public class WorkerResourcesDialog extends JDialog implements TableMouseListener {

    private static final long serialVersionUID = 20230810L;

    private ZiggyTable<PipelineDefinitionNode> nodeResourcesTable;
    private int selectedRow;
    private String pipelineDefinitionName;

    // Initial values in case the user decides to cancel the edits.
    private Map<PipelineDefinitionNode, WorkerResources> initialResources = new HashMap<>();

    public WorkerResourcesDialog(Window owner, PipelineDefinition pipelineDefinition) {
        super(owner, DEFAULT_MODALITY_TYPE);
        pipelineDefinitionName = pipelineDefinition.getName();
        for (PipelineDefinitionNode node : pipelineDefinition.getNodes()) {
            initialResources.put(node, node.workerResources());
        }

        nodeResourcesTable = new ZiggyTable<>(new WorkerResourcesTableModel(pipelineDefinition));
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Worker resources");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane()
            .add(createButtonPanel(createButton(CLOSE, "Close this dialog box.", this::close),
                createButton(CANCEL, "Cancel any changes made here and close dialog box.",
                    this::cancel)),
                BorderLayout.SOUTH);

        setMinimumSize(ZiggySwingUtils.MIN_DIALOG_SIZE);
        pack();
    }

    private JPanel createDataPanel() {
        WorkerResources resources = WorkerResources.getDefaultResources();

        JLabel pipeline = boldLabel("Pipeline");
        JLabel pipelineText = new JLabel(pipelineDefinitionName);

        JLabel defaultWorkerCount = boldLabel("Default worker count");
        JLabel defaultWorkerCountText = new JLabel(Integer.toString(resources.getMaxWorkerCount()));

        JLabel defaultHeapSize = boldLabel("Default worker heap size");
        JLabel defaultHeapSizeText = new JLabel(resources.humanReadableHeapSize().toString());

        ETable table = nodeResourcesTable.getTable();
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        ZiggySwingUtils.addTableMouseListener(table,
            createPopupMenu(createMenuItem(EDIT + DIALOG, WorkerResourcesDialog.this::edit)), this);
        JScrollPane nodeResourcesTableScrollPane = new JScrollPane(table);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(pipeline)
            .addComponent(pipelineText)
            .addComponent(defaultWorkerCount)
            .addComponent(defaultWorkerCountText)
            .addComponent(defaultHeapSize)
            .addComponent(defaultHeapSizeText)
            .addComponent(nodeResourcesTableScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(pipeline)
            .addComponent(pipelineText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(defaultWorkerCount)
            .addComponent(defaultWorkerCountText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(defaultHeapSize)
            .addComponent(defaultHeapSizeText)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(nodeResourcesTableScrollPane));

        return dataPanel;
    }

    @Override
    public void rowSelected(int row) {
        selectedRow = row;
    }

    @Override
    public void rowDoubleClicked(int row) {
        edit(row);
    }

    private void edit(ActionEvent evt) {
        edit(selectedRow);
    }

    private void edit(int row) {
        int modelRow = nodeResourcesTable.getTable().convertRowIndexToModel(row);
        try {
            new PipelineDefinitionNodeResourcesDialog(this, pipelineDefinitionName,
                nodeResourcesTable.getContentAtViewRow(modelRow)).setVisible(true);
            nodeResourcesTable.fireTableDataChanged();
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    /**
     * Closes the dialog box. Changes are kept, but can still be discarded in the edit pipelines
     * dialog box by selecting cancel.
     */
    private void close(ActionEvent evt) {
        setVisible(false);
    }

    /**
     * Resets all the pipeline definition node resource settings to their initial values on entry to
     * this dialog box and closes the dialog box.
     */
    private void cancel(ActionEvent evt) {
        for (Map.Entry<PipelineDefinitionNode, WorkerResources> entry : initialResources
            .entrySet()) {
            entry.getKey().applyWorkerResources(entry.getValue());
        }
        setVisible(false);
    }

    /**
     * Table model for the worker resources dialog box.
     *
     * @author PT
     */
    private static class WorkerResourcesTableModel
        extends AbstractZiggyTableModel<PipelineDefinitionNode> {

        private static final long serialVersionUID = 20230810L;

        private static final String[] COLUMN_NAMES = { "Name", "Max workers", "Heap size" };

        private List<PipelineDefinitionNode> pipelineDefinitionNodes = new ArrayList<>();

        public WorkerResourcesTableModel(PipelineDefinition pipelineDefinition) {
            pipelineDefinitionNodes = pipelineDefinition.getNodes();
            fireTableDataChanged();
        }

        @Override
        public Class<PipelineDefinitionNode> tableModelContentClass() {
            return PipelineDefinitionNode.class;
        }

        @Override
        public int getRowCount() {
            return pipelineDefinitionNodes.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return COLUMN_NAMES[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            WorkerResources resources = getContentAtRow(rowIndex).workerResources();
            return switch (columnIndex) {
                case 0 -> getContentAtRow(rowIndex).getModuleName();
                case 1 -> resources.maxWorkerCountIsDefault() ? "Default"
                    : Integer.toString(resources.getMaxWorkerCount());
                case 2 -> resources.heapSizeIsDefault() ? "Default"
                    : resources.humanReadableHeapSize().toString();
                default -> throw new PipelineException(
                    "Column index " + columnIndex + " not supported");
            };
        }

        @Override
        public PipelineDefinitionNode getContentAtRow(int row) {
            return pipelineDefinitionNodes.get(row);
        }
    }
}

package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingWorker;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel.ProcessingStatistics;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.ModuleTaskMetrics;

/**
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstanceStatsDialog extends javax.swing.JDialog {

    private ZiggyTable<ProcessingStatistics> processingTimeZiggyTable;
    private ZiggyTable<ModuleTaskMetrics> processingBreakdownZiggyTable;

    private final PipelineInstance pipelineInstance;
    private TaskMetricsTableModel processingBreakdownTableModel;
    private PipelineStatsTableModel processingTimeTableModel;
    private List<PipelineTaskDisplayData> tasks;
    private ArrayList<String> orderedModuleNames;

    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

    public InstanceStatsDialog(Window owner, PipelineInstance instance) {
        super(owner, DEFAULT_MODALITY_TYPE);

        pipelineInstance = instance;

        loadFromDatabase();

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void loadFromDatabase() {
        tasks = pipelineTaskDisplayDataOperations().pipelineTaskDisplayData(pipelineInstance);
        orderedModuleNames = new ArrayList<>();

        for (PipelineTaskDisplayData task : tasks) {
            String moduleName = task.getModuleName();
            if (!orderedModuleNames.contains(moduleName)) {
                orderedModuleNames.add(moduleName);
            }
        }
    }

    private void buildComponent() {
        try {
            setTitle("Pipeline instance performance statistics");

            getContentPane().add(createDataPanel(), BorderLayout.CENTER);
            getContentPane().add(ZiggySwingUtils.createButtonPanel(
                ZiggySwingUtils.createButton(REFRESH, this::refresh),
                ZiggySwingUtils.createButton(CLOSE, this::close)), BorderLayout.SOUTH);

            pack();
        } catch (Exception e) {
            throw e;
        }
    }

    private JPanel createDataPanel() {
        JLabel processingTime = ZiggySwingUtils.boldLabel("Processing time statistics",
            LabelType.HEADING);
        processingTimeZiggyTable = new ZiggyTable<>(
            new PipelineStatsTableModel(tasks, orderedModuleNames));
        JScrollPane processingTimeScrollPane = new JScrollPane(processingTimeZiggyTable.getTable());

        JLabel processingBreakdown = ZiggySwingUtils
            .boldLabel("Processing time breakdown for completed tasks", LabelType.HEADING);
        processingBreakdownZiggyTable = new ZiggyTable<>(
            new TaskMetricsTableModel(tasks, orderedModuleNames, false));
        JScrollPane processingBreakdownScrollPane = new JScrollPane(
            processingBreakdownZiggyTable.getTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(processingTime)
            .addComponent(processingTimeScrollPane)
            .addComponent(processingBreakdown)
            .addComponent(processingBreakdownScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(processingTime)
            .addComponent(processingTimeScrollPane)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(processingBreakdown)
            .addComponent(processingBreakdownScrollPane));

        return dataPanel;
    }

    private void refresh(ActionEvent evt) {
        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                loadFromDatabase();
                return null;
            }

            @Override
            protected void done() {
                processingTimeTableModel.update(tasks, orderedModuleNames);
                processingBreakdownTableModel.update(tasks, orderedModuleNames);
            }
        };
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    private static class PipelineStatsTableModel extends AbstractTableModel
        implements ModelContentClass<ProcessingStatistics> {

        private PipelineStatsDisplayModel pipelineStatsDisplayModel;

        public PipelineStatsTableModel(List<PipelineTaskDisplayData> tasks,
            List<String> orderedModuleNames) {
            update(tasks, orderedModuleNames);
        }

        public void update(List<PipelineTaskDisplayData> tasks, List<String> orderedModuleNames) {
            pipelineStatsDisplayModel = new PipelineStatsDisplayModel(tasks, orderedModuleNames);
            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            return pipelineStatsDisplayModel.getRowCount();
        }

        @Override
        public int getColumnCount() {
            return pipelineStatsDisplayModel.getColumnCount();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return pipelineStatsDisplayModel.getValueAt(rowIndex, columnIndex);
        }

        @Override
        public String getColumnName(int column) {
            return pipelineStatsDisplayModel.getColumnName(column);
        }

        @Override
        public Class<ProcessingStatistics> tableModelContentClass() {
            return ProcessingStatistics.class;
        }
    }
}

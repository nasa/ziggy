package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.proxy.PipelineTaskCrudProxy;

@SuppressWarnings("serial")
public class InstanceStatsDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(InstanceStatsDialog.class);

    private JPanel dataPanel;
    private JScrollPane processingBreakdownScrollPane;
    private JButton refreshButton;
    private JScrollPane processingTimeScrollPane;
    private JPanel processingBreakdownPanel;
    private JPanel processingTimePanel;
    private JButton closeButton;
    private JPanel buttonPanel;

    private ZTable processingBreakdownTable;
    private ZTable processingTimeTable;

    private final PipelineInstance pipelineInstance;
    private TaskMetricsTableModel processingBreakdownTableModel;
    private PipelineStatsTableModel processingTimeTableModel;
    private final PipelineTaskCrudProxy pipelineTaskCrud = new PipelineTaskCrudProxy();
    private List<PipelineTask> tasks;
    private ArrayList<String> orderedModuleNames;

    public InstanceStatsDialog(JFrame frame, PipelineInstance pipelineInstance) {
        super(frame, true);

        this.pipelineInstance = pipelineInstance;

        loadFromDatabase();
        initGUI();
    }

    public static void showInstanceStatsDialog(JFrame frame, PipelineInstance pipelineInstance) {
        InstanceStatsDialog dialog = new InstanceStatsDialog(frame, pipelineInstance);

        dialog.setVisible(true);
    }

    private void loadFromDatabase() {
        tasks = pipelineTaskCrud.retrieveAll(pipelineInstance);
        orderedModuleNames = new ArrayList<>();

        for (PipelineTask task : tasks) {
            String moduleName = task.getPipelineInstanceNode()
                .getPipelineModuleDefinition()
                .getName()
                .getName();
            if (!orderedModuleNames.contains(moduleName)) {
                orderedModuleNames.add(moduleName);
            }
        }
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        log.info("refreshButton.actionPerformed, event=" + evt);

        loadFromDatabase();
        updateTables();
    }

    private void updateTables() {
        processingTimeTableModel.update(tasks, orderedModuleNames);
        processingBreakdownTableModel.update(tasks, orderedModuleNames);
    }

    private void closeButtonActionPerformed(ActionEvent evt) {
        log.info("closeButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                setTitle("Pipeline Instance Performance Statistics");
                getContentPane().add(getDataPanel(), BorderLayout.CENTER);
                getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            }
            setSize(1280, 500);
        } catch (Exception e) {
            throw e;
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWidths = new int[] { 7 };
            dataPanelLayout.rowHeights = new int[] { 7, 7 };
            dataPanelLayout.columnWeights = new double[] { 0.1 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getProcessingTimePanel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getProcessingBreakdownPanel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(50);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getRefreshButton());
            buttonPanel.add(getCloseButton());
        }
        return buttonPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("<html><b>close</b></html>");
            closeButton.addActionListener(evt -> closeButtonActionPerformed(evt));
        }
        return closeButton;
    }

    private JPanel getProcessingTimePanel() {
        if (processingTimePanel == null) {
            processingTimePanel = new JPanel();
            BorderLayout processingTimePanelLayout = new BorderLayout();
            processingTimePanel.setLayout(processingTimePanelLayout);
            processingTimePanel
                .setBorder(BorderFactory.createTitledBorder("Processing Time Statistics"));
            processingTimePanel.add(getProcessingTimeScrollPane(), BorderLayout.CENTER);
        }
        return processingTimePanel;
    }

    private JPanel getProcessingBreakdownPanel() {
        if (processingBreakdownPanel == null) {
            processingBreakdownPanel = new JPanel();
            BorderLayout processingBreakdownPanelLayout = new BorderLayout();
            processingBreakdownPanel.setLayout(processingBreakdownPanelLayout);
            processingBreakdownPanel.setBorder(BorderFactory
                .createTitledBorder("Processing Time Breakdown (completed tasks only)"));
            processingBreakdownPanel.add(getProcessingBreakdownScrollPane(), BorderLayout.CENTER);
        }
        return processingBreakdownPanel;
    }

    private JScrollPane getProcessingTimeScrollPane() {
        if (processingTimeScrollPane == null) {
            processingTimeScrollPane = new JScrollPane();
            processingTimeScrollPane.setViewportView(getProcessingTimeTable());
        }
        return processingTimeScrollPane;
    }

    private JScrollPane getProcessingBreakdownScrollPane() {
        if (processingBreakdownScrollPane == null) {
            processingBreakdownScrollPane = new JScrollPane();
            processingBreakdownScrollPane.setViewportView(getProcessingBreakdownTable());
        }
        return processingBreakdownScrollPane;
    }

    private ZTable getProcessingTimeTable() {
        if (processingTimeTable == null) {
            processingTimeTableModel = new PipelineStatsTableModel(tasks, orderedModuleNames);
            processingTimeTable = new ZTable();
            processingTimeTable.setModel(processingTimeTableModel);
        }
        return processingTimeTable;
    }

    private ZTable getProcessingBreakdownTable() {
        if (processingBreakdownTable == null) {
            processingBreakdownTableModel = new TaskMetricsTableModel(tasks, orderedModuleNames,
                false);
            processingBreakdownTable = new ZTable();
            processingBreakdownTable.setModel(processingBreakdownTableModel);
        }
        return processingBreakdownTable;
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("refresh");
            refreshButton.addActionListener(evt -> refreshButtonActionPerformed(evt));
        }
        return refreshButton;
    }

}

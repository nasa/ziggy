package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.WorkerTaskLogInformationRequest;
import gov.nasa.ziggy.ui.proxy.PipelineTaskCrudProxy;

/**
 * Dialog box that presents the user with a table of all log files for a given task, with types,
 * sizes, etc. The user can then double-click on a row in the table to view the corresponding log
 * file in full.
 *
 * @author PT
 */
public class TaskLogInformationDialog extends JDialog implements MouseListener {

    private static final Logger log = LoggerFactory.getLogger(TaskLogInformationDialog.class);

    private static final long serialVersionUID = 20220325L;

    private final long taskId;
    private JButton closeButton;
    private JPanel actionPanel;
    private JPanel labelPanel;
    private JLabel taskLogLabel;
    private JButton refreshButton;
    private JTable taskLogTable;
    private JScrollPane taskLogScrollPane;
    private TaskLogInformationTableModel tableModel;

    public TaskLogInformationDialog(JFrame parent, PipelineTask pipelineTask) {
        super(parent, true);
        taskId = pipelineTask.getId();
        initGUI();
        refreshContents();
        setVisible(true);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setTitle("Task Log");
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getTaskLogScrollPane(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            getContentPane().add(getLabelPanel(), BorderLayout.NORTH);
            this.setSize(750, 500);
            setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // At the top of the window is the label panel.
    private JPanel getLabelPanel() {
        if (labelPanel == null) {
            labelPanel = new JPanel();
            GridBagLayout labelPanelLayout = new GridBagLayout();
            labelPanelLayout.rowWeights = new double[] { 0.1 };
            labelPanelLayout.rowHeights = new int[] {};
            labelPanelLayout.columnWeights = new double[] { 0.1 };
            labelPanelLayout.columnWidths = new int[] { 7 };
            labelPanel.setLayout(labelPanelLayout);
            labelPanel.add(getTaskLogLabel(), new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 10, 0, 0), 0, 0));
        }
        return labelPanel;
    }

    // The label panel contains the task label information.
    private JLabel getTaskLogLabel() {
        if (taskLogLabel == null) {
            taskLogLabel = new JLabel();
        }
        return taskLogLabel;
    }

    // In-between is the table itself
    private JScrollPane getTaskLogScrollPane() {
        if (taskLogScrollPane == null) {
            tableModel = new TaskLogInformationTableModel();
            taskLogTable = new JTable(tableModel);
            taskLogTable.setCellSelectionEnabled(false);
            taskLogTable.setRowSelectionAllowed(true);
            taskLogTable.addMouseListener(this);
            taskLogScrollPane = new JScrollPane(taskLogTable);
        }
        return taskLogScrollPane;
    }

    // At the bottom are some buttons: refresh and close.
    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(20);
            actionPanelLayout.setAlignment(FlowLayout.RIGHT);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getRefreshButton());
            actionPanel.add(getCloseButton());
        }
        return actionPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("Close");
            closeButton.addActionListener(evt -> closeButtonActionPerformed());
        }
        return closeButton;
    }

    private void closeButtonActionPerformed() {
        setVisible(false);
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("Refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        log.debug("refreshButton.actionPerformed, event=" + evt);

        refreshContents();
    }

    private void refreshContents() {

        // Get the pipeline task up-to-date information from the database.
        PipelineTask task = new PipelineTaskCrudProxy().retrieve(taskId);

        log.debug("selected task id = " + taskId);

        taskLogLabel.setText(task.taskLabelText());

        // Retrieve the task log information and put into the table model.
        tableModel
            .setTaskLogInformation(WorkerTaskLogInformationRequest.requestTaskLogInformation(task));

    }

    // MouseListener methods

    @Override
    public void mouseClicked(MouseEvent e) {

        // On double-click, select the desired row and retrieve the corresponding log.
        int row = taskLogTable.rowAtPoint(e.getPoint());
        if (e.getClickCount() == 2 && row != -1) {
            TaskLogInformation logInfo = tableModel.taskLogInformation(row);
            log.info("Obtaining log file: " + logInfo.getFilename());
            new SingleTaskLogDialog(this, logInfo);
        }
    }

    @Override
    public void mousePressed(MouseEvent e) {
        // do nothing special
    }

    @Override
    public void mouseReleased(MouseEvent e) {
        // do nothing special
    }

    @Override
    public void mouseEntered(MouseEvent e) {
        // do nothing special
    }

    @Override
    public void mouseExited(MouseEvent e) {
        // do nothing special
    }

    private static class TaskLogInformationTableModel extends AbstractTableModel {

        private static final long serialVersionUID = 20220325L;

        private static final String[] COLUMN_HEADINGS = { "Name", "Type", "Last Modified", "Size" };

        private List<TaskLogInformation> taskLogInformationList;

        public TaskLogInformationTableModel() {

        }

        public void setTaskLogInformation(Set<TaskLogInformation> taskLogInformation) {
            taskLogInformationList = new ArrayList<>(taskLogInformation.size());
            taskLogInformationList.addAll(taskLogInformation);
        }

        @Override
        public int getRowCount() {
            return taskLogInformationList.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_HEADINGS.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return COLUMN_HEADINGS[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            TaskLogInformation information = taskLogInformationList.get(rowIndex);
            switch (columnIndex) {
                case 0:
                    return information.getFilename();
                case 1:
                    return information.getLogType().toString();
                case 2:
                    return information.lastModifiedDateTime();
                case 3:
                    return information.logFileSizeEngineeringNotation();
                default:
                    throw new IllegalArgumentException("Illegal column number: " + columnIndex);
            }
        }

        public TaskLogInformation taskLogInformation(int rowIndex) {
            return taskLogInformationList.get(rowIndex);
        }

    }

}

package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.swing.GroupLayout;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingWorker;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.TaskLogInformationMessage;
import gov.nasa.ziggy.services.messages.TaskLogInformationRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.util.Requestor;

/**
 * Dialog box that presents the user with a table of all log files for a given task, with types,
 * sizes, etc. The user can then double-click on a row in the table to view the corresponding log
 * file in full.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskLogInformationDialog extends JDialog implements Requestor {

    private static final Logger log = LoggerFactory.getLogger(TaskLogInformationDialog.class);
    private static final long LOG_CONTENT_TIMEOUT_MILLIS = 2000L;

    private static final long serialVersionUID = 20240614L;

    private final long taskId;
    private JLabel instanceText;
    private JLabel workerText;
    private JLabel moduleText;
    private JLabel uowText;
    private JLabel elapsedTimeText;
    private JLabel processingStepText;
    private TaskLogInformationTableModel taskLogTableModel;
    private TaskLogInformationMessage currentMessage;

    private CountDownLatch taskInfoRequestCountdownLatch;

    private final UUID uuid = UUID.randomUUID();

    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public TaskLogInformationDialog(Window owner, long taskId) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.taskId = taskId;

        // Subscribe to TaskLogInformationMessage instances, since this panel needs to
        // get access to those messages.
        ZiggyMessenger.subscribe(TaskLogInformationMessage.class, message -> {
            if (isDestination(message)) {

                // Store the message so that our SwingWorker can access it, and count
                // down the CountDownLatch so the SwingWorker unblocks.
                currentMessage = message;
                taskInfoRequestCountdownLatch.countDown();
            }
        });
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Task log");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(ZiggySwingUtils.createButton(REFRESH, this::refresh),
            ZiggySwingUtils.createButton(CLOSE, this::close)), BorderLayout.SOUTH);

        refresh();
        pack();
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
    }

    private JPanel createDataPanel() {
        JLabel instance = boldLabel("ID:");
        instanceText = new JLabel();

        JLabel worker = boldLabel("Worker:");
        workerText = new JLabel();

        JLabel module = boldLabel("Task:");
        moduleText = new JLabel();

        JLabel uow = boldLabel("UOW:");
        uowText = new JLabel();

        JLabel processingStep = boldLabel("Processing step:");
        processingStepText = new JLabel();

        JLabel elapsedTime = boldLabel("Elapsed time:");
        elapsedTimeText = new JLabel();

        JScrollPane taskLogScrollPane = new JScrollPane(createTaskLogTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanelLayout.setAutoCreateGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(instance)
                    .addComponent(worker)
                    .addComponent(module)
                    .addComponent(uow)
                    .addComponent(processingStep)
                    .addComponent(elapsedTime))
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(instanceText)
                    .addComponent(workerText)
                    .addComponent(moduleText)
                    .addComponent(uowText)
                    .addComponent(processingStepText)
                    .addComponent(elapsedTimeText)))
            .addComponent(taskLogScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(instance)
                .addComponent(instanceText))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(worker).addComponent(workerText))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(module).addComponent(moduleText))
            .addGroup(dataPanelLayout.createParallelGroup().addComponent(uow).addComponent(uowText))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(processingStep)
                .addComponent(processingStepText))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(elapsedTime)
                .addComponent(elapsedTimeText))
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(taskLogScrollPane));

        return dataPanel;
    }

    private JTable createTaskLogTable() {
        taskLogTableModel = new TaskLogInformationTableModel();
        JTable taskLogTable = new JTable(taskLogTableModel);
        taskLogTable.setPreferredScrollableViewportSize(new Dimension(750, 300));
        taskLogTable.setCellSelectionEnabled(false);
        taskLogTable.setRowSelectionAllowed(true);
        taskLogTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent evt) {

                // On double-click, select the desired row and retrieve the corresponding log.
                int row = taskLogTable.rowAtPoint(evt.getPoint());
                if (evt.getClickCount() == 2 && row != -1) {
                    TaskLogInformation logInfo = taskLogTableModel.taskLogInformation(row);
                    log.info("Obtaining log file {}", logInfo.getFilename());
                    try {
                        new SingleTaskLogDialog(TaskLogInformationDialog.this, logInfo)
                            .setVisible(true);
                    } catch (Throwable e) {
                        MessageUtils.showError(TaskLogInformationDialog.this, e.getMessage(), e);
                    }
                }
            }
        });

        return taskLogTable;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private void refresh(ActionEvent evt) {
        refresh();
    }

    /**
     * Refreshes the {@link TaskLogInformationDialog}.
     */
    public void refresh() {
        new SwingWorker<Set<TaskLogInformation>, Void>() {

            @Override
            protected Set<TaskLogInformation> doInBackground() throws Exception {

                // Get the pipeline task up-to-date information from the database.
                PipelineTaskDisplayData task = pipelineTaskDisplayDataOperations()
                    .pipelineTaskDisplayData(pipelineTaskOperations().pipelineTask(taskId));
                log.debug("Selected task ID is {}", taskId);
                instanceText.setText(Long.toString(task.getPipelineInstanceId()));
                workerText.setText(task.getWorkerName());
                moduleText.setText(task.getModuleName());
                uowText.setText(task.getBriefState());
                processingStepText.setText(HtmlBuilder.htmlBuilder()
                    .appendBoldColor(task.getDisplayProcessingStep(),
                        task.isError() ? "red" : "green")
                    .toString());
                elapsedTimeText.setText(task.getExecutionClock().toString());

                // Request the task log information.
                taskInfoRequestCountdownLatch = new CountDownLatch(1);
                ZiggyMessenger.publish(new TaskLogInformationRequest(TaskLogInformationDialog.this,
                    task.getPipelineTask()));

                // Wait for the task log to be delivered, but don't wait too long.
                if (!taskInfoRequestCountdownLatch.await(LOG_CONTENT_TIMEOUT_MILLIS,
                    TimeUnit.MILLISECONDS)) {
                    MessageUtils.showError(rootPane, "Request for task log list timed out.");
                    return null;
                }

                return currentMessage.taskLogInformation();
            }

            @Override
            public void done() {
                try {
                    Set<TaskLogInformation> taskLogInformation = get();
                    if (taskLogInformation == null) {
                        return;
                    }
                    taskLogTableModel.setTaskLogInformation(taskLogInformation);
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(rootPane, e);
                }
            }
        }.execute();
    }

    @Override
    public UUID requestorIdentifier() {
        return uuid;
    }

    private PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    private static class TaskLogInformationTableModel extends AbstractTableModel {

        private static final long serialVersionUID = 20240614L;

        private static final String[] COLUMN_NAMES = { "Name", "Type", "Modified", "Size" };

        private List<TaskLogInformation> taskLogInformationList = new ArrayList<>();

        public void setTaskLogInformation(Set<TaskLogInformation> taskLogInformation) {
            taskLogInformationList = new ArrayList<>(taskLogInformation.size());
            taskLogInformationList.addAll(taskLogInformation);
            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            return taskLogInformationList.size();
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
            TaskLogInformation information = taskLogInformationList.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> information.getFilename();
                case 1 -> information.getLogType().toString();
                case 2 -> information.lastModifiedDateTime();
                case 3 -> information.logFileSizeEngineeringNotation();
                default -> throw new IllegalArgumentException(
                    "Illegal column number: " + columnIndex);
            };
        }

        public TaskLogInformation taskLogInformation(int rowIndex) {
            return taskLogInformationList.get(rowIndex);
        }
    }
}

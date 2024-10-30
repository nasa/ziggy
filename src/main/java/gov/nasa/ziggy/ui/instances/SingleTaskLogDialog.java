package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.TO_BOTTOM;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.TO_TOP;
import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.SingleTaskLogMessage;
import gov.nasa.ziggy.services.messages.SingleTaskLogRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.util.Requestor;

/**
 * @author Bill Wohler
 */
public class SingleTaskLogDialog extends javax.swing.JDialog implements Requestor {

    private static final Logger log = LoggerFactory.getLogger(TaskLogInformationDialog.class);
    private static final long serialVersionUID = 20240614L;
    private static final long LOG_CONTENT_TIMEOUT_MILLIS = 2000L;

    private JLabel taskLogLabel;
    private JTextArea textArea;
    private JScrollPane textScrollPane;

    private TaskLogInformation taskLogInformation;
    private SingleTaskLogMessage currentMessage;
    private CountDownLatch taskLogMessageCountdownLatch;

    private final UUID uuid = UUID.randomUUID();

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public SingleTaskLogDialog(Window owner, TaskLogInformation taskLogInformation) {

        super(owner, ModalityType.MODELESS);
        this.taskLogInformation = taskLogInformation;

        // Subscribe to the task log messages before refreshing the window.
        ZiggyMessenger.subscribe(SingleTaskLogMessage.class, message -> {
            if (isDestination(message)) {
                currentMessage = message;
                taskLogMessageCountdownLatch.countDown();
            }
        });
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle(taskLogInformation.getFilename() + " - Task log");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(ZiggySwingUtils.createButton(REFRESH, this::refresh),
            ZiggySwingUtils.createButton(CLOSE, this::close)), BorderLayout.SOUTH);

        refresh();
        pack();
    }

    private JPanel createDataPanel() {
        taskLogLabel = new JLabel();

        JPanel navButtons = createButtonPanel(ZiggySwingUtils.createButton(TO_TOP, this::topOfLog),
            ZiggySwingUtils.createButton(TO_BOTTOM, this::bottomOfLog));

        textArea = new JTextArea();
        textArea.setEditable(false);

        textScrollPane = new JScrollPane(textArea);
        textScrollPane.setPreferredSize(new Dimension(1300, 800));

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGap(10) // just enough to get the label away from the edge
                .addComponent(taskLogLabel)
                .addComponent(navButtons))
            .addComponent(textScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(dataPanelLayout.createParallelGroup(Alignment.CENTER)
                .addComponent(taskLogLabel)
                .addComponent(navButtons, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE))
            .addComponent(textScrollPane));

        return dataPanel;
    }

    private void topOfLog(ActionEvent evt) {
        JScrollBar verticalScrollBar = textScrollPane.getVerticalScrollBar();
        verticalScrollBar.setValue(verticalScrollBar.getMinimum());
    }

    private void bottomOfLog(ActionEvent evt) {
        JScrollBar verticalScrollBar = textScrollPane.getVerticalScrollBar();
        verticalScrollBar.setValue(verticalScrollBar.getMaximum());
    }

    private void refresh(ActionEvent evt) {
        refresh();
    }

    /**
     * Refreshes the contents of the dialog box.
     */
    private void refresh() {
        new SwingWorker<String, Void>() {

            @Override
            protected String doInBackground() throws Exception {

                // Retrieve the task from the database
                PipelineTaskDisplayData task = pipelineTaskDisplayDataOperations()
                    .pipelineTaskDisplayData(
                        pipelineTaskOperations().pipelineTask(taskLogInformation.getTaskId()));
                log.debug("Selected task ID is {}", task.getPipelineTaskId());
                taskLogLabel.setText(taskLabelText(task));

                // Request the log contents from the supervisor
                taskLogMessageCountdownLatch = new CountDownLatch(1);
                SingleTaskLogRequest.requestSingleTaskLog(SingleTaskLogDialog.this,
                    taskLogInformation);

                // Wait for the task log to be delivered, but don't wait too long.
                if (!taskLogMessageCountdownLatch.await(LOG_CONTENT_TIMEOUT_MILLIS,
                    TimeUnit.MILLISECONDS)) {
                    MessageUtils.showError(rootPane, "Log request timed out.");
                    return null;
                }

                // Strip the task log out of the message, and then send the message
                // itself to Davy Jones' Locker.
                String taskLogContents = currentMessage.taskLogContents();
                currentMessage = null;
                return taskLogContents;
            }

            /** Returns the label for a task that is used in some UI displays. */
            private String taskLabelText(PipelineTaskDisplayData pipelineTaskDisplayData) {
                return htmlBuilder().appendBold("ID: ")
                    .append(pipelineTaskDisplayData.getPipelineInstanceId())
                    .append(":")
                    .append(pipelineTaskDisplayData.getPipelineTaskId())
                    .appendBold(" WORKER: ")
                    .append(pipelineTaskDisplayData.getWorkerName())
                    .appendBold(" TASK: ")
                    .append(pipelineTaskDisplayData.getModuleName())
                    .append(" [")
                    .append(pipelineTaskDisplayData.getBriefState())
                    .append("] ")
                    .appendBoldColor(pipelineTaskDisplayData.getDisplayProcessingStep(),
                        pipelineTaskDisplayData.isError() ? "red" : "green")
                    .append(" ")
                    .appendItalic(pipelineTaskDisplayData.getExecutionClock().toString())
                    .toString();
            }

            @Override
            protected void done() {
                try {
                    String updatedText = get();
                    if (updatedText == null) {
                        return;
                    }
                    textArea.setText(updatedText);
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(rootPane, e);
                }
            }
        }.execute();
    }

    private void close(ActionEvent evt) {
        setVisible(false);
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
}

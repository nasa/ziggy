package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messages.WorkerSingleTaskLogRequest;
import gov.nasa.ziggy.ui.proxy.PipelineTaskCrudProxy;

public class SingleTaskLogDialog extends javax.swing.JDialog {

    private static final Logger log = LoggerFactory.getLogger(TaskLogInformationDialog.class);

    private static final long serialVersionUID = -8926788606491576183L;

    private JPanel textPanel;
    private JButton refreshButton;
    private JPanel upperButtonPanel;
    private JLabel taskLogLabel;
    private JPanel labelPanel;
    private JTextArea textArea;
    private JScrollPane textScrollPane;
    private JButton closeButton;
    private JPanel actionPanel;
    private JButton topOfLogButton;
    private JButton bottomOfLogButton;

    private TaskLogInformation taskLogInformation;

    public SingleTaskLogDialog(JDialog frame, TaskLogInformation taskLogInformation) {
        super(frame, false);
        this.taskLogInformation = taskLogInformation;
        initGUI();
        refreshContents();
        setVisible(true);
    }

    private void refreshContents() {

        long taskId = taskLogInformation.getTaskId();
        PipelineTask task = new PipelineTaskCrudProxy().retrieve(taskId);

        log.debug("selected task id = " + taskId);

        taskLogLabel.setText(task.taskLabelText());

        String taskLogs = WorkerSingleTaskLogRequest.requestSingleTaskLog(taskLogInformation);
        textArea.setText(taskLogs);
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        log.debug("refreshButton.actionPerformed, event=" + evt);

        refreshContents();
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setTitle("Task Log: " + taskLogInformation.getFilename());
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getTextPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            getContentPane().add(getLabelPanel(), BorderLayout.NORTH);
            this.setSize(1192, 879);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getTextPanel() {
        if (textPanel == null) {
            textPanel = new JPanel();
            BorderLayout textPanelLayout = new BorderLayout();
            textPanel.setLayout(textPanelLayout);
            textPanel.add(getTextScrollPane(), BorderLayout.CENTER);
        }
        return textPanel;
    }

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

    private JScrollPane getTextScrollPane() {
        if (textScrollPane == null) {
            textScrollPane = new JScrollPane();
            textScrollPane.setViewportView(getTextArea());
        }
        return textScrollPane;
    }

    private JTextArea getTextArea() {
        if (textArea == null) {
            textArea = new JTextArea();
            textArea.setEditable(false);
        }
        return textArea;
    }

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
            labelPanel.add(getUpperButtonPanel(), new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        }
        return labelPanel;
    }

    private JLabel getTaskLogLabel() {
        if (taskLogLabel == null) {
            taskLogLabel = new JLabel();
        }
        return taskLogLabel;
    }

    private JPanel getUpperButtonPanel() {
        if (upperButtonPanel == null) {
            upperButtonPanel = new JPanel();
            FlowLayout upperButtonPanelLayout = new FlowLayout();
            upperButtonPanelLayout.setHgap(20);
            upperButtonPanelLayout.setAlignment(FlowLayout.RIGHT);
            upperButtonPanel.setLayout(upperButtonPanelLayout);
            upperButtonPanel.add(getTopOfLogButton());
            upperButtonPanel.add(getBottomOfLogButton());
        }
        return upperButtonPanel;
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("Refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }

    private JButton getTopOfLogButton() {
        if (topOfLogButton == null) {
            topOfLogButton = new JButton();
            topOfLogButton.setText("To Top");
            topOfLogButton.addActionListener(this::topOfLogButtonActionPerformed);
        }
        return topOfLogButton;
    }

    private void topOfLogButtonActionPerformed(ActionEvent evt) {
        JScrollBar vertical = textScrollPane.getVerticalScrollBar();
        vertical.setValue(vertical.getMinimum());
    }

    private JButton getBottomOfLogButton() {
        if (bottomOfLogButton == null) {
            bottomOfLogButton = new JButton();
            bottomOfLogButton.setText("To Bottom");
            bottomOfLogButton.addActionListener(this::bottomOfLogButtonActionPerformed);
        }
        return bottomOfLogButton;
    }

    private void bottomOfLogButtonActionPerformed(ActionEvent evt) {
        JScrollBar vertical = textScrollPane.getVerticalScrollBar();
        vertical.setValue(vertical.getMaximum());
    }

}

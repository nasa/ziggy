package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.TableCellEditor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class RestartDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(RestartDialog.class);

    private JPanel dataPanel;
    private JButton restartButton;
    private JTable restartTable;
    private JScrollPane scrollPane;
    private JButton cancelButton;
    private JPanel buttonPanel;
    private RestartTableModel restartTableModel;
    private final List<PipelineTask> failedTasks;
    private final Map<Long, ProcessingSummary> taskAttrs;
    private boolean cancelled = false;

    public RestartDialog(JFrame frame, List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {
        super(frame, true);

        this.failedTasks = failedTasks;
        this.taskAttrs = taskAttrs;

        initGUI();
    }

    public static RunMode restartTasks(JFrame frame, List<PipelineTask> failedTasks,
        Map<Long, ProcessingSummary> taskAttrs) {
        RestartDialog dialog = new RestartDialog(frame, failedTasks, taskAttrs);
        dialog.setVisible(true); // blocks until user presses a button
        RestartAttributes restartAttrs = null;

        if (dialog.cancelled) {
            log.info("Restart cancelled by user");
            return null;
        }
        Map<String, RestartAttributes> moduleMap = dialog.restartTableModel.getModuleMap();

        for (PipelineTask failedTask : failedTasks) {
            String moduleName = failedTask.getModuleName();

            ProcessingSummary attrs = taskAttrs.get(failedTask.getId());
            String pState = ProcessingState.INITIALIZING.toString();

            if (attrs != null) {
                pState = attrs.getProcessingState().shortName();
            }

            String key = RestartAttributes.key(moduleName, pState);

            restartAttrs = moduleMap.get(key);

            log.info("Set task " + failedTask.getId() + " restartMode to "
                + restartAttrs.getSelectedRestartMode());
        }
        return restartAttrs.getSelectedRestartMode();
    }

    private void restartButtonActionPerformed(ActionEvent evt) {
        log.debug("restartButton.actionPerformed, event=" + evt);
        setVisible(false);
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);
        cancelled = true;
        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                setTitle("Restart Failed Tasks");
                getContentPane().add(getDataPanel(), BorderLayout.CENTER);
                getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            }
            this.setSize(740, 567);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getScrollPane(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(100);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getRestartButton());
            buttonPanel.add(getCancelButton());
        }
        return buttonPanel;
    }

    private JButton getRestartButton() {
        if (restartButton == null) {
            restartButton = new JButton();
            restartButton.setText("Restart");
            restartButton.addActionListener(this::restartButtonActionPerformed);
        }
        return restartButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }

    private JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getRestartTable());
        }
        return scrollPane;
    }

    private JTable getRestartTable() {
        if (restartTable == null) {
            restartTableModel = new RestartTableModel(failedTasks, taskAttrs);

            List<RestartAttributes> modules = restartTableModel.getModuleList();
            final List<TableCellEditor> editors = new ArrayList<>();

            for (RestartAttributes module : modules) {
                JComboBox<RunMode> comboBox1 = new JComboBox<>(
                    new Vector<>(module.getRestartModes()));
                DefaultCellEditor dce1 = new DefaultCellEditor(comboBox1);
                editors.add(dce1);
            }

            restartTable = new JTable(restartTableModel) {
                // Determine editor to be used by row
                @Override
                public TableCellEditor getCellEditor(int row, int column) {
                    int modelColumn = convertColumnIndexToModel(column);

                    if (modelColumn == 3) {
                        return editors.get(row);
                    }
                    return super.getCellEditor(row, column);
                }
            };
        }
        return restartTable;
    }

    /**
     * @return the cancelled
     */
    public boolean isCancelled() {
        return cancelled;
    }
}

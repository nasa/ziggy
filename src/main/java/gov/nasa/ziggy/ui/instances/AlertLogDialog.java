package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingWorker;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.services.alert.AlertLogOperations;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.AlertLogDisplayModel;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class AlertLogDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(AlertLogDialog.class);

    public AlertLogDialog(Window owner, long pipelineInstanceId) {
        super(owner, DEFAULT_MODALITY_TYPE);

        buildComponent(pipelineInstanceId);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(long pipelineInstanceId) {
        setTitle("Alerts");

        getContentPane().add(createDataPanel(pipelineInstanceId), BorderLayout.CENTER);
        getContentPane().add(ZiggySwingUtils.createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);
        setPreferredSize(ZiggyGuiConstants.MIN_DIALOG_SIZE);

        pack();
    }

    private JPanel createDataPanel(long pipelineInstanceId) {
        ZiggyTable<AlertLog> ziggyTable = new ZiggyTable<>(
            new AlertLogTableModel(pipelineInstanceId));
        JScrollPane tableScrollPane = new JScrollPane(ziggyTable.getTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(
            dataPanelLayout.createParallelGroup().addComponent(tableScrollPane));

        dataPanelLayout.setVerticalGroup(
            dataPanelLayout.createSequentialGroup().addComponent(tableScrollPane));

        return dataPanel;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private static class AlertLogTableModel extends AbstractTableModel
        implements ModelContentClass<AlertLog> {

        private final long pipelineInstanceId;
        private final AlertLogDisplayModel alertLogDisplayModel = new AlertLogDisplayModel();

        private final AlertLogOperations alertLogOperations = new AlertLogOperations();

        public AlertLogTableModel(long pipelineInstanceId) {
            this.pipelineInstanceId = pipelineInstanceId;
            loadFromDatabase();
        }

        public void loadFromDatabase() {
            new SwingWorker<List<AlertLog>, Void>() {
                @Override
                protected List<AlertLog> doInBackground() throws Exception {
                    return alertLogOperations().alertLogs(pipelineInstanceId);
                }

                @Override
                protected void done() {
                    try {
                        alertLogDisplayModel.update(get());
                        fireTableDataChanged();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Could not retrieve alerts", e);
                    }
                }
            }.execute();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return alertLogDisplayModel.getValueAt(rowIndex, columnIndex);
        }

        @Override
        public int getColumnCount() {
            return alertLogDisplayModel.getColumnCount();
        }

        @Override
        public int getRowCount() {
            return alertLogDisplayModel.getRowCount();
        }

        @Override
        public String getColumnName(int column) {
            return alertLogDisplayModel.getColumnName(column);
        }

        @Override
        public Class<AlertLog> tableModelContentClass() {
            return AlertLog.class;
        }

        private AlertLogOperations alertLogOperations() {
            return alertLogOperations;
        }
    }
}

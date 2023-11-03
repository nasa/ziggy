package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.models.TableModelContentClass;
import gov.nasa.ziggy.ui.util.proxy.AlertLogCrudProxy;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.AlertLogDisplayModel;

/**
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class AlertLogDialog extends javax.swing.JDialog {

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

        setMinimumSize(ZiggySwingUtils.MIN_DIALOG_SIZE);
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
        implements TableModelContentClass<AlertLog> {
        private final AlertLogCrudProxy alertLogCrud;
        private List<AlertLog> alerts = new ArrayList<>();
        private final long pipelineInstanceId;
        private final AlertLogDisplayModel alertLogDisplayModel = new AlertLogDisplayModel();

        public AlertLogTableModel(long pipelineInstanceId) {
            this.pipelineInstanceId = pipelineInstanceId;

            alertLogCrud = new AlertLogCrudProxy();

            loadFromDatabase();
        }

        public void loadFromDatabase() {
            try {
                alerts = alertLogCrud.retrieveForPipelineInstance(pipelineInstanceId);
                alertLogDisplayModel.update(alerts);
            } catch (ConsoleSecurityException ignore) {
            }

            fireTableDataChanged();
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
    }
}

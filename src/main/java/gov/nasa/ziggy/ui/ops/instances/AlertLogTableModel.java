package gov.nasa.ziggy.ui.ops.instances;

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.proxy.AlertLogCrudProxy;
import gov.nasa.ziggy.util.dispmod.AlertLogDisplayModel;

@SuppressWarnings("serial")
public class AlertLogTableModel extends AbstractTableModel {
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
}

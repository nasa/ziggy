package gov.nasa.ziggy.ui.dr;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.data.management.DataReceiptFile;
import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.DataReceiptOperationsProxy;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Displays detailed information on data receipt actions from a single instance.
 *
 * @author PT
 * @author Bill Wohler
 */
public class DataReceiptInstanceDialog extends JDialog {

    private static final long serialVersionUID = 20230823L;
    ZiggyTable<DataReceiptFile> ziggyTable;

    public DataReceiptInstanceDialog(Window owner, DataReceiptInstance dataReceiptInstance) {
        super(owner, ModalityType.MODELESS);
        buildComponent(dataReceiptInstance);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(DataReceiptInstance dataReceiptInstance) {
        setTitle("Data receipt details");

        getContentPane().add(createDataPanel(dataReceiptInstance), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(REFRESH, this::refresh),
            createButton(CLOSE, this::close)), BorderLayout.SOUTH);

        pack();
    }

    private Component createDataPanel(DataReceiptInstance dataReceiptInstance) {

        JLabel instance = boldLabel("Instance:");
        JLabel instanceText = new JLabel(Long.toString(dataReceiptInstance.getInstanceId()));

        JLabel date = boldLabel("Date:");
        JLabel dateText = new JLabel(dataReceiptInstance.getDate().toString());

        JLabel successful = boldLabel("Successful:");
        JLabel successfulText = new JLabel(
            Integer.toString(dataReceiptInstance.getSuccessfulImportCount()));

        JLabel failed = boldLabel("Failed:");
        JLabel failedText = new JLabel(
            Integer.toString(dataReceiptInstance.getFailedImportCount()));

        ziggyTable = new ZiggyTable<>(new DataReceiptInstanceTableModel(dataReceiptInstance));
        for (int column = 0; column < DataReceiptInstanceTableModel.COLUMN_WIDTHS.length; column++) {
            ziggyTable.setPreferredColumnWidth(column,
                DataReceiptInstanceTableModel.COLUMN_WIDTHS[column]);
        }
        JScrollPane tableScrollPane = new JScrollPane(ziggyTable.getTable());

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(instance)
                    .addComponent(date)
                    .addComponent(successful)
                    .addComponent(failed))
                .addPreferredGap(ComponentPlacement.RELATED)
                .addGroup(dataPanelLayout.createParallelGroup()
                    .addComponent(instanceText)
                    .addComponent(dateText)
                    .addComponent(successfulText)
                    .addComponent(failedText)))
            .addComponent(tableScrollPane));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(instance)
                .addComponent(instanceText))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(date).addComponent(dateText))
            .addGroup(dataPanelLayout.createParallelGroup()
                .addComponent(successful)
                .addComponent(successfulText))
            .addGroup(
                dataPanelLayout.createParallelGroup().addComponent(failed).addComponent(failedText))
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(tableScrollPane));

        return dataPanel;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private void refresh(ActionEvent evt) {
        ziggyTable.loadFromDatabase();
    }

    private static class DataReceiptInstanceTableModel
        extends AbstractDatabaseModel<DataReceiptFile> {

        private static final long serialVersionUID = 20230823L;

        private static final String[] COLUMN_NAMES = { "Task ID", "Name", "Status" };
        private static final int[] COLUMN_WIDTHS = { 100, 500, 100 };

        private List<DataReceiptFile> dataReceiptFiles = new ArrayList<>();
        private DataReceiptInstance dataReceiptInstance;

        public DataReceiptInstanceTableModel(DataReceiptInstance dataReceiptInstance) {
            this.dataReceiptInstance = dataReceiptInstance;
            loadFromDatabase();
        }

        @Override
        public int getRowCount() {
            return dataReceiptFiles.size();
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
            DataReceiptFile dataReceiptFile = dataReceiptFiles.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> dataReceiptFile.getTaskId();
                case 1 -> dataReceiptFile.getName();
                case 2 -> dataReceiptFile.getStatus();
                default -> throw new IllegalArgumentException(
                    "Invalid column index: " + columnIndex);
            };
        }

        @Override
        public void loadFromDatabase() {
            dataReceiptFiles = new DataReceiptOperationsProxy()
                .dataReceiptFilesForInstance(dataReceiptInstance.getInstanceId());
            fireTableDataChanged();
        }

        @Override
        public DataReceiptFile getContentAtRow(int row) {
            return dataReceiptFiles.get(row);
        }

        @Override
        public Class<DataReceiptFile> tableModelContentClass() {
            return DataReceiptFile.class;
        }
    }
}

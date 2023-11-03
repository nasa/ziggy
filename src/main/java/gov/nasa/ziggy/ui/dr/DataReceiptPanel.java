package gov.nasa.ziggy.ui.dr;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.DataReceiptOperationsProxy;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;

/**
 * Panel that displays summary information for all pipeline instances that performed a data receipt
 * operation.
 *
 * @author PT
 * @author Bill Wohler
 */
public class DataReceiptPanel extends JPanel {

    private static final long serialVersionUID = 20230823L;

    private ZiggyTable<DataReceiptInstance> ziggyTable;

    public DataReceiptPanel() {
        buildComponent();
    }

    /**
     * Initializes the panel. The panel has a button panel on the top that provides the refresh
     * button, and a scroll pane below the button panel that provides the table of data receipt
     * instances.
     */
    protected void buildComponent() {
        JPanel buttonPanel = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(REFRESH, this::refresh));

        ziggyTable = createDataReceiptTable();
        JScrollPane tableScrollPane = new JScrollPane(ziggyTable.getTable());

        GroupLayout layout = new GroupLayout(this);
        setLayout(layout);

        layout.setHorizontalGroup(
            layout.createParallelGroup().addComponent(buttonPanel).addComponent(tableScrollPane));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(buttonPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(tableScrollPane));
    }

    private ZiggyTable<DataReceiptInstance> createDataReceiptTable() {
        ZiggyTable<DataReceiptInstance> ziggyTable = new ZiggyTable<>(new DataReceiptTableModel());
        ziggyTable.loadFromDatabase();
        ziggyTable.addMouseListener(new MouseAdapter() {

            /**
             * Launches a dialog box that contains the table of files imported for a given instance.
             */
            @Override
            public void mouseClicked(MouseEvent evt) {
                // On double-click, select the desired row and retrieve the corresponding log.
                int row = ziggyTable.rowAtPoint(evt.getPoint());
                if (evt.getClickCount() == 2 && row != -1) {
                    try {
                        new DataReceiptInstanceDialog(
                            SwingUtilities.getWindowAncestor(DataReceiptPanel.this),
                            ziggyTable.getContentAtViewRow(row)).setVisible(true);
                    } catch (Throwable e) {
                        MessageUtil
                            .showError(SwingUtilities.getWindowAncestor(DataReceiptPanel.this), e);
                    }
                }
            }
        });
        return ziggyTable;
    }

    private void refresh(ActionEvent evt) {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private static class DataReceiptTableModel extends AbstractDatabaseModel<DataReceiptInstance> {

        private static final long serialVersionUID = 20230823L;

        private static final String[] COLUMN_NAMES = { "Instance", "Date", "Successful", "Failed" };

        private List<DataReceiptInstance> dataReceiptInstances = new ArrayList<>();

        @Override
        public int getRowCount() {
            return dataReceiptInstances.size();
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
            DataReceiptInstance dataReceiptInstance = dataReceiptInstances.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> dataReceiptInstance.getInstanceId();
                case 1 -> dataReceiptInstance.getDate();
                case 2 -> dataReceiptInstance.getSuccessfulImportCount();
                case 3 -> dataReceiptInstance.getFailedImportCount();
                default -> throw new IllegalArgumentException(
                    "Invalid column index: " + columnIndex);
            };
        }

        @Override
        public void loadFromDatabase() {
            dataReceiptInstances = new DataReceiptOperationsProxy().dataReceiptInstances();
            fireTableDataChanged();
        }

        @Override
        public DataReceiptInstance getContentAtRow(int row) {
            return dataReceiptInstances.get(row);
        }

        @Override
        public Class<DataReceiptInstance> tableModelContentClass() {
            return DataReceiptInstance.class;
        }
    }
}

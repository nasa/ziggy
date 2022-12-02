package gov.nasa.ziggy.ui.config.datareceipt;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.data.management.DataReceiptFile;
import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.DataReceiptOperationsProxy;

/**
 * Displays detailed information on data receipt actions from a single instance.
 *
 * @author PT
 */
public class DataReceiptInstanceDialog extends JDialog {

    private static final long serialVersionUID = 20220624L;

    private static final String[] COLUMN_HEADINGS = { "Task ID", "Name", "Type", "Status" };

    private final DataReceiptInstance dataReceiptInstance;
    private JScrollPane scrollPane;
    private ZTable dataReceiptTable;
    private DataReceiptInstanceTableModel tableModel;
    private JPanel actionPanel;
    private JButton closeButton;
    private JButton refreshButton;
    private JPanel labelPanel;
    private JLabel instanceLabel;

    public DataReceiptInstanceDialog(JFrame parent, DataReceiptInstance dataReceiptInstance) {
        super(parent, false);
        this.dataReceiptInstance = dataReceiptInstance;
        initGUI();
        refreshContents();
        setVisible(true);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getScrollPane(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            getContentPane().add(getLabelPanel(), BorderLayout.NORTH);
            this.setSize(900, 800);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getTable());
        }
        return scrollPane;
    }

    private ZTable getTable() {
        if (dataReceiptTable == null) {
            dataReceiptTable = new ZTable();
            dataReceiptTable.setTextWrappingEnabled(true);
            dataReceiptTable.setRowShadingEnabled(true);
            dataReceiptTable.setModel(getTableModel());
            dataReceiptTable.getColumnModel().getColumn(0).setPreferredWidth(100);
            dataReceiptTable.getColumnModel().getColumn(2).setPreferredWidth(100);
            dataReceiptTable.getColumnModel().getColumn(3).setPreferredWidth(100);
            dataReceiptTable.getColumnModel().getColumn(1).setPreferredWidth(500);

        }
        return dataReceiptTable;
    }

    private DataReceiptInstanceTableModel getTableModel() {
        if (tableModel == null) {
            tableModel = new DataReceiptInstanceTableModel();
        }
        return tableModel;
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

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("Refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }

    private JPanel getLabelPanel() {
        if (labelPanel == null) {
            labelPanel = new JPanel();
            labelPanel.add(getInstanceLabel());
        }
        return labelPanel;
    }

    private JLabel getInstanceLabel() {
        if (instanceLabel == null) {
            instanceLabel = new JLabel();
            instanceLabel.setText("Instance " + dataReceiptInstance.getInstanceId()
                + " processed at " + dataReceiptInstance.getDate() + " imported files = "
                + dataReceiptInstance.getSuccessfulImportCount() + " failed imports = "
                + dataReceiptInstance.getFailedImportCount());
        }
        return instanceLabel;
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        refreshContents();
    }

    private void refreshContents() {
        getTableModel().loadFromDatabase();
    }

    private class DataReceiptInstanceTableModel extends AbstractDatabaseModel {

        private static final long serialVersionUID = 20220624L;

        private List<DataReceiptFile> dataReceiptFiles = new ArrayList<>();

        @Override
        public int getRowCount() {
            return dataReceiptFiles.size();
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
            DataReceiptFile dataReceiptFile = dataReceiptFiles.get(rowIndex);
            switch (columnIndex) {
                case 0:
                    return dataReceiptFile.getTaskId();
                case 1:
                    return dataReceiptFile.getName();
                case 2:
                    return dataReceiptFile.getFileType();
                case 3:
                    return dataReceiptFile.getStatus();
                default:
                    throw new IllegalArgumentException("Invalid column index: " + columnIndex);
            }
        }

        @Override
        public void loadFromDatabase() {
            dataReceiptFiles = new DataReceiptOperationsProxy()
                .dataReceiptFilesForInstance(dataReceiptInstance.getInstanceId());
            fireTableDataChanged();
        }

    }

}

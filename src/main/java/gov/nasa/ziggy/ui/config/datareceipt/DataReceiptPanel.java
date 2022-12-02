package gov.nasa.ziggy.ui.config.datareceipt;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.DataReceiptOperationsProxy;

/**
 * Panel that displays summary information for all pipeline instances that performed a data receipt
 * operation.
 *
 * @author PT
 */
public class DataReceiptPanel extends JPanel implements MouseListener {

    private static final long serialVersionUID = 20220624L;

    private JScrollPane scrollPane;
    private JPanel buttonPanel;
    private JButton refreshButton;
    private ZTable dataReceiptTable;
    private DataReceiptTableModel tableModel;

    public DataReceiptPanel() {
        super();
        initGUI();
        tableModel.loadFromDatabase();
    }

    /**
     * Initializes the panel. The panel has a button panel on the top that provides the refresh
     * button, and a scroll pane below the button panel that provides the table of data receipt
     * instances.
     */
    protected void initGUI() {
        BorderLayout thisLayout = new BorderLayout();
        setLayout(thisLayout);
        setPreferredSize(new Dimension(400, 300));
        this.add(getScrollPane(), BorderLayout.CENTER);
        this.add(getButtonPanel(), BorderLayout.NORTH);
    }

    private JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getTable());
        }
        return scrollPane;
    }

    protected JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setAlignment(FlowLayout.LEFT);
            buttonPanelLayout.setHgap(20);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getRefreshButton());
        }
        return buttonPanel;
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        try {
            getTableModel().loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private ZTable getTable() {
        if (dataReceiptTable == null) {
            dataReceiptTable = new ZTable();
            dataReceiptTable.setRowShadingEnabled(true);
            dataReceiptTable.setTextWrappingEnabled(true);
            dataReceiptTable.setModel(getTableModel());
            dataReceiptTable.addMouseListener(this);
        }
        return dataReceiptTable;
    }

    private DataReceiptTableModel getTableModel() {
        if (tableModel == null) {
            tableModel = new DataReceiptTableModel();
        }
        return tableModel;
    }

    /**
     * Launches a dialog box that contains the table of files imported for a given instance.
     */
    @Override
    public void mouseClicked(MouseEvent e) {
        // On double-click, select the desired row and retrieve the corresponding log.
        int row = getTable().rowAtPoint(e.getPoint());
        if (e.getClickCount() == 2 && row != -1) {
            ZiggyGuiConsole.newDataReceiptInstanceDialog(getTableModel().dataReceiptInstance(row));
        }

    }

    @Override
    public void mousePressed(MouseEvent e) {
    }

    @Override
    public void mouseReleased(MouseEvent e) {
    }

    @Override
    public void mouseEntered(MouseEvent e) {
    }

    @Override
    public void mouseExited(MouseEvent e) {
    }

    private static class DataReceiptTableModel extends AbstractDatabaseModel {

        private static final long serialVersionUID = 20220624L;

        private static final String[] COLUMN_HEADINGS = { "Instance", "Date", "# Successful",
            "# Failed" };

        private List<DataReceiptInstance> dataReceiptInstances = new ArrayList<>();

        @Override
        public int getRowCount() {
            return dataReceiptInstances.size();
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
            DataReceiptInstance dataReceiptInstance = dataReceiptInstances.get(rowIndex);
            switch (columnIndex) {
                case 0:
                    return dataReceiptInstance.getInstanceId();
                case 1:
                    return dataReceiptInstance.getDate();
                case 2:
                    return dataReceiptInstance.getSuccessfulImportCount();
                case 3:
                    return dataReceiptInstance.getFailedImportCount();
                default:
                    throw new IllegalArgumentException("Invalid column index: " + columnIndex);
            }
        }

        @Override
        public void loadFromDatabase() {
            dataReceiptInstances = new DataReceiptOperationsProxy().DataReceiptInstances();
            fireTableDataChanged();
        }

        public DataReceiptInstance dataReceiptInstance(int rowIndex) {
            return dataReceiptInstances.get(rowIndex);
        }

    }

}

package gov.nasa.ziggy.ui.config.events;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.services.events.ZiggyEvent;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.ZiggyEventCrudProxy;

/**
 * Panel to display events detected and managed by {@link ZiggyEventHandler}.
 *
 * @author PT
 */
public class ZiggyEventPanel extends JPanel {

    private static final long serialVersionUID = 20220708L;

    private JScrollPane scrollPane;
    private JPanel buttonPanel;
    private JButton refreshButton;
    private ZTable eventTable;
    private EventTableModel tableModel;

    public ZiggyEventPanel() {
        super();
        initGUI();
        getTableModel().loadFromDatabase();
    }

    private void initGUI() {
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
        if (eventTable == null) {
            eventTable = new ZTable();
            eventTable.setRowShadingEnabled(true);
            eventTable.setTextWrappingEnabled(true);
            eventTable.setModel(getTableModel());
        }
        return eventTable;
    }

    private EventTableModel getTableModel() {
        if (tableModel == null) {
            tableModel = new EventTableModel();
        }
        return tableModel;
    }

    private static class EventTableModel extends AbstractDatabaseModel {

        private static final long serialVersionUID = 20220708L;

        public static final String[] COLUMN_NAMES = new String[] { "ID", "Handler Name",
            "Pipeline Name", "Date", "Instance ID" };

        List<ZiggyEvent> ziggyEvents = new ArrayList<>();

        @Override
        public int getRowCount() {
            return ziggyEvents.size();
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
            ZiggyEvent ziggyEvent = ziggyEvents.get(rowIndex);
            switch (columnIndex) {
                case 0:
                    return ziggyEvent.getId();
                case 1:
                    return ziggyEvent.getEventHandlerName();
                case 2:
                    return ziggyEvent.getPipelineName().getName();
                case 3:
                    return ziggyEvent.getEventTime();
                case 4:
                    return ziggyEvent.getPipelineInstanceId();
                default:
                    throw new IllegalArgumentException("Column index " + columnIndex + " invalid");
            }
        }

        @Override
        public void loadFromDatabase() {
            ziggyEvents = new ZiggyEventCrudProxy().retrieveAllEvents();
            fireTableDataChanged();
        }

    }

}

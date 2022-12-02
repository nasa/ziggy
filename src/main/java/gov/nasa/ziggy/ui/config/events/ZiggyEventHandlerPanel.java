package gov.nasa.ziggy.ui.config.events;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventHandler.ZiggyEventHandlerInfoForDisplay;
import gov.nasa.ziggy.services.messages.EventHandlerRequest;
import gov.nasa.ziggy.services.messages.EventHandlerToggleStateRequest;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;

/**
 * Panel to display the collection of {@link ZiggyEventHandler} instances and their states.
 *
 * @author PT
 */
public class ZiggyEventHandlerPanel extends JPanel implements MouseListener {

    private static final Logger log = LoggerFactory.getLogger(ZiggyEventHandlerPanel.class);

    private static final long serialVersionUID = 20220707L;

    private JScrollPane scrollPane;
    private JPanel buttonPanel;
    private JButton refreshButton;
    private ZTable eventHandlerTable;
    private EventHandlerTableModel tableModel;

    public ZiggyEventHandlerPanel() {
        super();
        initGUI();
        getTableModel().update();
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
            getTableModel().update();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private ZTable getTable() {
        if (eventHandlerTable == null) {
            eventHandlerTable = new ZTable();
            eventHandlerTable.setRowShadingEnabled(true);
            eventHandlerTable.setTextWrappingEnabled(true);
            eventHandlerTable.setModel(getTableModel());
            eventHandlerTable.getColumnModel().getColumn(0).setPreferredWidth(100);
            eventHandlerTable.getColumnModel().getColumn(1).setPreferredWidth(500);
            eventHandlerTable.getColumnModel().getColumn(2).setPreferredWidth(100);
            eventHandlerTable.getColumnModel().getColumn(3).setPreferredWidth(100);
            eventHandlerTable.addMouseListener(this);
        }
        return eventHandlerTable;
    }

    private EventHandlerTableModel getTableModel() {
        if (tableModel == null) {
            tableModel = new EventHandlerTableModel();
        }
        return tableModel;
    }

    @Override
    public void mouseClicked(MouseEvent e) {

        // does the user want to enable or disable an event handler?
        int row = getTable().rowAtPoint(e.getPoint());
        int column = getTable().columnAtPoint(e.getPoint());
        log.debug("row: " + row + ", column: " + column);
        if (row != -1 && column == 3) {
            EventHandlerToggleStateRequest.requestEventHandlerToggle(getTableModel().getName(row));
            getTableModel().update();
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

    private static class EventHandlerTableModel extends AbstractTableModel {

        private static final long serialVersionUID = 20220707L;

        private static final String[] COLUMN_HEADERS = new String[] { "Name", "Directory",
            "Pipeline", "Enabled" };

        List<ZiggyEventHandlerInfoForDisplay> eventHandlers = new ArrayList<>();

        public String getName(int rowIndex) {
            return eventHandlers.get(rowIndex).getName();
        }

        @Override
        public int getRowCount() {
            return eventHandlers.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_HEADERS.length;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return COLUMN_HEADERS[columnIndex];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            ZiggyEventHandlerInfoForDisplay eventHandler = eventHandlers.get(rowIndex);
            switch (columnIndex) {
                case 0:
                    return eventHandler.getName();
                case 1:
                    return eventHandler.getDirectory();
                case 2:
                    return eventHandler.getPipelineName();
                case 3:
                    return eventHandler.isEnabled();
                default:
                    throw new IllegalArgumentException("Illegal column: " + columnIndex);
            }
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            switch (columnIndex) {
                case 0:
                case 1:
                case 2:
                    return String.class;
                case 3:
                    return Boolean.class;
                default:
                    return String.class;
            }
        }

        @SuppressWarnings("unchecked")
        public void update() {
            eventHandlers.clear();
            eventHandlers
                .addAll((Collection<? extends ZiggyEventHandlerInfoForDisplay>) UiCommunicator
                    .send(new EventHandlerRequest()));
            fireTableDataChanged();
        }
    }

}

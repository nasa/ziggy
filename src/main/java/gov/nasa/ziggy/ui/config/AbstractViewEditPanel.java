package gov.nasa.ziggy.ui.config;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public abstract class AbstractViewEditPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(AbstractViewEditPanel.class);

    protected JScrollPane scrollPane;
    protected JPopupMenu popupMenu;
    protected JMenuItem editMenuItem;
    protected JMenuItem newMenuItem;
    protected JMenuItem deleteMenuItem;
    protected JButton refreshButton;
    protected JButton newButton;
    private JPanel buttonPanel;
    protected ZTable table;
    protected int selectedModelRow = -1;

    public AbstractViewEditPanel() {
    }

    protected abstract AbstractTableModel getTableModel() throws PipelineUIException;

    protected abstract void doEdit(int row);

    protected abstract void doDelete(int row);

    protected abstract void doNew();

    protected abstract void doRefresh();

    protected abstract String getEditMenuText();

    protected abstract String getNewMenuText();

    protected abstract String getDeleteMenuText();

    private void newButtonActionPerformed(ActionEvent evt) {
        log.debug("newButton.actionPerformed, event=" + evt);

        try {
            doNew();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        log.debug("refreshButton.actionPerformed, event=" + evt);

        try {
            doRefresh();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void editMenuItemActionPerformed(ActionEvent evt) {
        log.debug("editMenuItem.actionPerformed, event=" + evt);
        log.debug("[PU] table row =" + selectedModelRow);

        try {
            doEdit(selectedModelRow);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void tableMouseClicked(MouseEvent evt) {
        log.debug("tableMouseClicked(MouseEvent) - start");

        if (evt.getClickCount() == 2) {
            log.debug(
                "tableMouseClicked(MouseEvent) - [DOUBLE-CLICK] table.mouseClicked, event=" + evt);
            int tableRow = table.rowAtPoint(evt.getPoint());
            selectedModelRow = table.convertRowIndexToModel(tableRow);
            log.debug("tableMouseClicked(MouseEvent) - [DC] table row =" + selectedModelRow);

            try {
                doEdit(selectedModelRow);
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }

        log.debug("tableMouseClicked(MouseEvent) - end");
    }

    private void deleteMenuItemActionPerformed(ActionEvent evt) {
        log.debug("deleteMenuItem.actionPerformed, event=" + evt);

        try {
            doDelete(selectedModelRow);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void newMenuItemActionPerformed(ActionEvent evt) {
        log.debug("newMenuItem.actionPerformed, event=" + evt);

        try {
            doNew();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    protected void initGUI() throws PipelineUIException {
        log.debug("initGUI() - start");

        BorderLayout thisLayout = new BorderLayout();
        setLayout(thisLayout);
        setPreferredSize(new Dimension(400, 300));
        this.add(getScrollPane(), BorderLayout.CENTER);
        this.add(getButtonPanel(), BorderLayout.NORTH);

        log.debug("initGUI() - end");
    }

    private JScrollPane getScrollPane() throws PipelineUIException {
        log.debug("getUsersPanelScrollPane() - start");

        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getTable());
        }

        log.debug("getUsersPanelScrollPane() - end");
        return scrollPane;
    }

    private ZTable getTable() throws PipelineUIException {
        log.debug("getUsersTable() - start");

        if (table == null) {
            table = new ZTable();
            table.setRowShadingEnabled(true);
            table.setModel(getTableModel());
            table.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent evt) {
                    tableMouseClicked(evt);
                }
            });
            setComponentPopupMenu(table, getPopupMenu());
        }

        log.debug("getUsersTable() - end");
        return table;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        log.debug("setComponentPopupMenu(java.awt.Component, javax.swing.JPopupMenu) - start");

        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                    int tableRow = table.rowAtPoint(e.getPoint());
                    // windows bug? works ok on Linux/gtk. Here's a workaround:
                    if (tableRow == -1) {
                        tableRow = table.getSelectedRow();
                    }
                    selectedModelRow = table.convertRowIndexToModel(tableRow);
                }
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }
        });

        log.debug("setComponentPopupMenu(java.awt.Component, javax.swing.JPopupMenu) - end");
    }

    protected JPopupMenu getPopupMenu() {
        log.debug("getUserMenu() - start");

        if (popupMenu == null) {
            popupMenu = new JPopupMenu();
            popupMenu.add(getEditMenuItem());
            popupMenu.add(getDeleteMenuItem());
            popupMenu.add(getNewMenuItem());
        }

        log.debug("getUserMenu() - end");
        return popupMenu;
    }

    private JMenuItem getEditMenuItem() {
        log.debug("getEditMenuItem() - start");

        if (editMenuItem == null) {
            editMenuItem = new JMenuItem();
            editMenuItem.setText(getEditMenuText());
            editMenuItem.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                editMenuItemActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getEditMenuItem() - end");
        return editMenuItem;
    }

    private JMenuItem getDeleteMenuItem() {
        log.debug("getDeleteMenuItem() - start");

        if (deleteMenuItem == null) {
            deleteMenuItem = new JMenuItem();
            deleteMenuItem.setText(getDeleteMenuText());
            deleteMenuItem.addActionListener(this::deleteMenuItemActionPerformed);
        }

        log.debug("getDeleteMenuItem() - end");
        return deleteMenuItem;
    }

    private JMenuItem getNewMenuItem() {
        log.debug("getNewMenuItem() - start");

        if (newMenuItem == null) {
            newMenuItem = new JMenuItem();
            newMenuItem.setText(getNewMenuText());
            newMenuItem.addActionListener(this::newMenuItemActionPerformed);
        }

        log.debug("getNewMenuItem() - end");
        return newMenuItem;
    }

    protected JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setAlignment(FlowLayout.LEFT);
            buttonPanelLayout.setHgap(20);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getNewButton());
            buttonPanel.add(getRefreshButton());
        }
        return buttonPanel;
    }

    private JButton getNewButton() {
        if (newButton == null) {
            newButton = new JButton();
            newButton.setText("new");
            newButton.addActionListener(this::newButtonActionPerformed);
        }
        return newButton;
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }
}

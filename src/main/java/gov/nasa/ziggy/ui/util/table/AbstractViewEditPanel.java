package gov.nasa.ziggy.ui.util.table;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ASSIGN_GROUP;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.COLLAPSE_ALL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.COPY;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DELETE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EXPAND_ALL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.NEW;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.RENAME;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.VIEW;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.Icon;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.table.TableModel;

import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.HasGroup;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.GroupsDialog;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.ui.util.proxy.CrudProxy;
import gov.nasa.ziggy.ui.util.proxy.RetrieveLatestVersionsCrudProxy;

/**
 * Provides a framework for a Swing panel that includes a table and a button panel. The panel allows
 * the user to edit the objects that are displayed in the table, create new objects, delete objects
 * (unless they are locked), and to refresh the table contents.
 * <p>
 * Subclasses of {@link AbstractViewEditPanel} provide a context menu that includes options to
 * create a new object, edit an object, or delete an object. They also all include buttons to
 * refresh the display and to create a new object. In addition, there are three optional actions
 * that the panel can provide: copying an object, renaming an object, and assigning an object to a
 * group. The selection of optional functions is controlled by the
 * {@link #optionalViewEditFunctions()} method, which returns a {@link Set} of instances of
 * {@link OptionalViewEditFunctions}:
 * <ol>
 * <li>A subclass will support copying table objects if the {@link #optionalViewEditFunctions()}
 * returns a {@link Set} that includes {@link OptionalViewEditFunctions#COPY}. In addition, the
 * {@link #copy(int)} method must be overridden. A copy option will be added to the context menu.
 * <li>A subclass will support renaming table objects if the {@link #optionalViewEditFunctions()}
 * returns a {@link Set} that includes {@link OptionalViewEditFunctions#RENAME}. In addition, the
 * {@link #rename(int)} method must be overridden. A rename option will be added to the context
 * menu.
 * <li>A subclass will support assigning table objects to groups if the
 * {@link #optionalViewEditFunctions()} returns a {@link Set} that includes
 * {@link OptionalViewEditFunctions#GROUP}. A group option will be added to the context menu, and
 * expand all and collapse all buttons will be added to the button panel. Finally, the class of
 * objects that are handled by the table must implement the {@link HasGroup} interface.
 * </ol>
 *
 * @author Todd Klaus
 * @author PT
 */
@SuppressWarnings("serial")
public abstract class AbstractViewEditPanel<T> extends JPanel {

    public enum OptionalViewEditFunctions {
        NEW, VIEW, GROUP, COPY, RENAME, DELETE;
    }

    protected ZiggyTable<T> ziggyTable;
    private ETable table;
    protected int selectedModelRow = -1;
    private JScrollPane scrollPane;
    private JPopupMenu popupMenu;
    private JPanel buttonPanel;

    public AbstractViewEditPanel(TableModel tableModel) {
        ziggyTable = new ZiggyTable<>(tableModel);
        table = ziggyTable.getTable();
    }

    public AbstractViewEditPanel(RowModel rowModel, ZiggyTreeModel<?> treeModel,
        String nodesColumnLabel) {
        ziggyTable = new ZiggyTable<>(rowModel, treeModel, nodesColumnLabel);
        table = ziggyTable.getTable();
    }

    protected void buildComponent() {
        setLayout(new BorderLayout());
        setPreferredSize(new Dimension(400, 300));
        add(getButtonPanel(), BorderLayout.NORTH);
        add(getScrollPane(), BorderLayout.CENTER);
    }

    protected JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR, null,
                createButton(REFRESH, this::refresh),
                optionalViewEditFunctions().contains(OptionalViewEditFunctions.NEW)
                    ? createButton(NEW, this::newItem)
                    : null,
                optionalViewEditFunctions().contains(OptionalViewEditFunctions.GROUP)
                    ? createButton(EXPAND_ALL, this::expandAll)
                    : null,
                optionalViewEditFunctions().contains(OptionalViewEditFunctions.GROUP)
                    ? createButton(COLLAPSE_ALL, this::collapseAll)
                    : null);
        }
        return buttonPanel;
    }

    private void newItem(ActionEvent evt) {
        try {
            create();
        } catch (Throwable e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
        }
    }

    private void refresh(ActionEvent evt) {
        refresh();
    }

    private void expandAll(ActionEvent evt) {
        ziggyTable.expandAll();
    }

    private void collapseAll(ActionEvent evt) {
        ziggyTable.collapseAll();
    }

    protected JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane(table);
            table.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent evt) {
                    tableMouseClicked(evt);
                }
            });
            setComponentPopupMenu(table, getPopupMenu());
        }
        return scrollPane;
    }

    private void tableMouseClicked(MouseEvent evt) {
        int tableRow = table.rowAtPoint(evt.getPoint());
        selectedModelRow = table.convertRowIndexToModel(tableRow);
        if (evt.getClickCount() == 2) {
            try {
                edit(selectedModelRow);
            } catch (Exception e) {
                MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final Component parent, final JPopupMenu menu) {

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
    }

    protected JPopupMenu getPopupMenu() {
        if (popupMenu == null) {
            popupMenu = new JPopupMenu();
            addMenuItem(popupMenu, OptionalViewEditFunctions.NEW, getNewMenuItem());
            addMenuItem(popupMenu, OptionalViewEditFunctions.VIEW, getViewMenuItem());
            popupMenu.add(getEditMenuItem());
            addMenuItem(popupMenu, OptionalViewEditFunctions.GROUP, getGroupMenuItem());
            addMenuItem(popupMenu, OptionalViewEditFunctions.COPY, getCopyMenuItem());
            addMenuItem(popupMenu, OptionalViewEditFunctions.RENAME, getRenameMenuItem());
            addMenuItem(popupMenu, OptionalViewEditFunctions.DELETE, getDeleteMenuItem());
        }

        return popupMenu;
    }

    private void addMenuItem(JPopupMenu popupMenu, OptionalViewEditFunctions function,
        JMenuItem menuItem) {
        if (optionalViewEditFunctions().contains(function)) {
            popupMenu.add(menuItem);
        }
    }

    protected Set<OptionalViewEditFunctions> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunctions.DELETE, OptionalViewEditFunctions.NEW);
    }

    private JMenuItem getNewMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(NEW + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    create();
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    private JMenuItem getViewMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(VIEW + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    view(selectedModelRow);
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    private JMenuItem getEditMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(EDIT + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    edit(selectedModelRow);
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    private JMenuItem getGroupMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(ASSIGN_GROUP + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    group();
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    private JMenuItem getCopyMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(COPY + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    copy(selectedModelRow);
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    private JMenuItem getRenameMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(RENAME + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    rename(selectedModelRow);
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    private JMenuItem getDeleteMenuItem() {
        return new JMenuItem(new ViewEditPanelAction(DELETE + DIALOG, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    delete(selectedModelRow);
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(panel), e);
                }
            }
        });
    }

    protected abstract void refresh();

    protected abstract void create();

    protected void view(int row) {
    }

    protected abstract void edit(int row);

    /**
     * Assign objects in the table to a selected {@link Group}.
     */
    protected void group() {
        checkPrivileges();
        try {
            Group group = GroupsDialog.selectGroup(this);
            if (group == null) {
                return;
            }
            List<T> selectedObjects = ziggyTable.getContentAtSelectedRows();
            if (!selectedObjects.isEmpty() && !(selectedObjects.get(0) instanceof HasGroup)) {
                throw new UnsupportedOperationException("Grouping not permitted");
            }
            for (T object : selectedObjects) {
                HasGroup groupableObject = (HasGroup) object;
                if (group == Group.DEFAULT) {
                    groupableObject.setGroup(null);
                } else {
                    groupableObject.setGroup(group);
                }
                getCrudProxy().update(object);
            }
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    protected void copy(int row) {
    }

    protected void rename(int row) {
    }

    protected abstract void delete(int row);

    protected RetrieveLatestVersionsCrudProxy<T> getCrudProxy() {
        return null;
    }

    protected void checkPrivileges() {
        try {
            CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
            return;
        }
    }

    /**
     * Extension of {@link AbstractAction} that allows a reference to the parent panel to be passed
     * to the {@link AbstractAction#actionPerformed(ActionEvent)} method.
     *
     * @author PT
     */
    private abstract class ViewEditPanelAction extends AbstractAction {

        protected Component panel;

        public ViewEditPanelAction(String name, Icon icon) {
            super(name, icon);
            panel = AbstractViewEditPanel.this;
        }
    }
}

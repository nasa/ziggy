package gov.nasa.ziggy.ui.util.table;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.COPY;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DELETE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.table.TableModel;

import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
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
 * {@link OptionalViewEditFunction}:
 * <ol>
 * <li>A subclass will support copying table objects if the {@link #optionalViewEditFunctions()}
 * returns a {@link Set} that includes {@link OptionalViewEditFunction#COPY}. In addition, the
 * {@link #copy(int)} method must be overridden. A copy option will be added to the context menu.
 * <li>A subclass will support renaming table objects if the {@link #optionalViewEditFunctions()}
 * returns a {@link Set} that includes {@link OptionalViewEditFunction#RENAME}. In addition, the
 * {@link #rename(int)} method must be overridden. A rename option will be added to the context
 * menu.
 * </ol>
 *
 * @author Todd Klaus
 * @author PT
 */
@SuppressWarnings("serial")
public abstract class AbstractViewEditPanel<T> extends JPanel {

    public enum OptionalViewEditFunction {
        NEW, VIEW, COPY, RENAME, DELETE;
    }

    protected ZiggyTable<T> ziggyTable;
    private ETable table;
    protected int selectedModelRow = -1;
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

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR, null,
                createButton(REFRESH, this::refresh),
                optionalViewEditFunctions().contains(OptionalViewEditFunction.NEW)
                    ? createButton(NEW, this::newItem)
                    : null);
        }
        for (JButton button : buttons()) {
            ZiggySwingUtils.addButtonsToPanel(buttonPanel, button);
        }
        return buttonPanel;
    }

    /**
     * Additional buttons that must be added to the button panel.
     * <p>
     * This method is provided so that subclasses of {@link AbstractViewEditPanel} can supply
     * additional buttons that they need on the button panel. Classes that require buttons should
     * override this method.
     * <p>
     * Because a superclass may have also added buttons, the subclass should prepend or append their
     * buttons to {@code super.buttons()} as appropriate.
     */
    protected List<JButton> buttons() {
        return new ArrayList<>();
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

    protected JScrollPane getScrollPane() {
        JScrollPane scrollPane = new JScrollPane(table);
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent evt) {
                tableMouseClicked(evt);
            }
        });
        setComponentPopupMenu(table, getPopupMenu());
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

    private JPopupMenu getPopupMenu() {

        JPopupMenu popupMenu = new JPopupMenu();
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.NEW, getNewMenuItem());
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.VIEW, getViewMenuItem());
        popupMenu.add(getEditMenuItem());
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.COPY, getCopyMenuItem());
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.RENAME, getRenameMenuItem());
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.DELETE, getDeleteMenuItem());

        for (JMenuItem menuItem : menuItems()) {
            popupMenu.add(menuItem);
        }
        return popupMenu;
    }

    /**
     * Adds additional, optional menu items to the context menu. Subclasses that need such menu
     * items should override this method.
     * <p>
     * Because a superclass may have also added menu items, the subclass should prepend or append
     * their menu items to {@code super.menuItems()} as appropriate.
     */
    protected List<JMenuItem> menuItems() {
        return new ArrayList<>();
    }

    private void addOptionalMenuItem(JPopupMenu popupMenu, OptionalViewEditFunction function,
        JMenuItem menuItem) {
        if (optionalViewEditFunctions().contains(function)) {
            popupMenu.add(menuItem);
        }
    }

    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunction.DELETE, OptionalViewEditFunction.NEW);
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

    protected void copy(int row) {
    }

    protected void rename(int row) {
    }

    protected abstract void delete(int row);

    protected RetrieveLatestVersionsCrudProxy<T> getCrudProxy() {
        return null;
    }

    /**
     * Extension of {@link AbstractAction} that allows a reference to the parent panel to be passed
     * to the {@link AbstractAction#actionPerformed(ActionEvent)} method.
     *
     * @author PT
     */
    abstract class ViewEditPanelAction extends AbstractAction {

        protected Component panel;

        public ViewEditPanelAction(String name, Icon icon) {
            super(name, icon);
            panel = AbstractViewEditPanel.this;
        }
    }
}

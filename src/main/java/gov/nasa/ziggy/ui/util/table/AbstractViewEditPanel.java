package gov.nasa.ziggy.ui.util.table;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REFRESH;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.VIEW;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.HierarchyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.table.TableModel;
import javax.swing.tree.TreePath;

import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.outline.Outline;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Provides a framework for a Swing panel that includes a table and a button panel. The panel allows
 * the user to edit the objects that are displayed in the table and to refresh the table contents.
 * <p>
 * Subclasses of {@link AbstractViewEditPanel} provide a context menu that includes options to view
 * or edit an object. They also all include buttons to refresh the display. In addition, the panel
 * can provide an optional action to assign an object to a group. The selection of optional
 * functions is controlled by the {@link #optionalViewEditFunctions()} method, which returns a
 * {@link Set} of instances of {@link OptionalViewEditFunction}:
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public abstract class AbstractViewEditPanel<T> extends JPanel {

    public enum OptionalViewEditFunction {
        REFRESH, VIEW, EDIT;

        @Override
        public String toString() {
            return ZiggyStringUtils.constantToSentenceWithSpaces(super.toString());
        }
    }

    protected ZiggyTable<T> ziggyTable;
    private ETable table;
    protected int selectedModelRow = -1;

    private Map<OptionalViewEditFunction, Action> actionByFunction = panelActions();
    private JPanel buttonPanel;

    public AbstractViewEditPanel(TableModel tableModel) {
        this(new ZiggyTable<>(tableModel));
    }

    public AbstractViewEditPanel(RowModel rowModel, ZiggyTreeModel<?> treeModel,
        String nodesColumnLabel) {
        this(new ZiggyTable<>(rowModel, treeModel, nodesColumnLabel));
    }

    private AbstractViewEditPanel(ZiggyTable<T> ziggyTable) {
        this.ziggyTable = ziggyTable;
        table = getTable(ziggyTable);

        buildComponent();
        addHierarchyListener(this::hierarchyListener);
    }

    /**
     * Obtain the table from the {@code ZiggyTable} and set some general things.
     * <p>
     * Tables are set up with {@link ListSelectionModel#SINGLE_SELECTION} by default. The selection
     * model can be updated if necessary.
     */
    private ETable getTable(ZiggyTable<T> ziggyTable) {
        ETable table = ziggyTable.getTable();
        table.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        return table;
    }

    private void buildComponent() {
        setLayout(new BorderLayout());
        setPreferredSize(new Dimension(400, 300));
        add(getButtonPanel(), BorderLayout.NORTH);
        add(getScrollPane(), BorderLayout.CENTER);
    }

    // Ensure panel is current whenever it is made visible.
    private void hierarchyListener(HierarchyEvent evt) {
        JComponent component = (JComponent) evt.getSource();
        if ((evt.getChangeFlags() & HierarchyEvent.SHOWING_CHANGED) != 0 && component.isShowing()) {
            refresh();
        }
    }

    private Map<OptionalViewEditFunction, Action> panelActions() {
        Map<OptionalViewEditFunction, Action> panelActions = new HashMap<>();

        panelActions.put(OptionalViewEditFunction.REFRESH, new AbstractAction(REFRESH, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    refresh();
                } catch (Exception e) {
                    MessageUtils
                        .showError(SwingUtilities.getWindowAncestor(AbstractViewEditPanel.this), e);
                }
            }
        });

        panelActions.put(OptionalViewEditFunction.VIEW, new AbstractAction(VIEW, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    view(selectedModelRow);
                } catch (Exception e) {
                    MessageUtils
                        .showError(SwingUtilities.getWindowAncestor(AbstractViewEditPanel.this), e);
                }
            }
        });

        panelActions.put(OptionalViewEditFunction.EDIT, new AbstractAction(EDIT, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    edit(selectedModelRow);
                } catch (Exception e) {
                    MessageUtils
                        .showError(SwingUtilities.getWindowAncestor(AbstractViewEditPanel.this), e);
                }
            }
        });

        return panelActions;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR, null,
                createButton(getActionByFunction().get(OptionalViewEditFunction.REFRESH)));
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

    protected JScrollPane getScrollPane() {
        // Remove the existing table mouse listener as it clears the selection with Control-Click on
        // the Mac rather than preserving the selection before it displays a menu. Perform selection
        // ourselves using ZiggySwingUtils.adjustSelection().
        for (MouseListener l : table.getMouseListeners()) {
            if (l.getClass().getName().equals("javax.swing.plaf.basic.BasicTableUI$Handler")) {
                table.removeMouseListener(l);
            }
        }
        table.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent evt) {
                ZiggySwingUtils.adjustSelection(table, evt);

                updateActionState(actionByFunction);

                if (evt.isPopupTrigger()) {
                    displayPopupMenu(evt);
                } else if (table instanceof Outline) {
                    toggleExpansionState((Outline) table, evt);
                }
            }

            @Override
            public void mouseClicked(MouseEvent evt) {
                selectAndEditRow(evt);
            }
        });

        return new JScrollPane(table);
    }

    /**
     * Updates the actions used in the buttons or context menu items. It is called any time the
     * selection changes.
     */
    protected void updateActionState(Map<OptionalViewEditFunction, Action> actionByFunction) {
    }

    private void displayPopupMenu(java.awt.event.MouseEvent evt) {
        selectedModelRow = table.convertRowIndexToModel(table.getSelectedRow());
        T contentAtViewRow = ziggyTable.getContentAtViewRow(selectedModelRow);
        if (contentAtViewRow != null) {
            getPopupMenu().show(table, evt.getX(), evt.getY());
        }
    }

    private JPopupMenu getPopupMenu() {

        JPopupMenu popupMenu = new JPopupMenu();
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.VIEW,
            menuItem(OptionalViewEditFunction.VIEW));
        popupMenu.add(menuItem(OptionalViewEditFunction.EDIT));

        for (JMenuItem menuItem : menuItems()) {
            popupMenu.add(menuItem);
        }
        return popupMenu;
    }

    private void addOptionalMenuItem(JPopupMenu popupMenu, OptionalViewEditFunction function,
        JMenuItem menuItem) {
        if (optionalViewEditFunctions().contains(function)) {
            popupMenu.add(menuItem);
        }
    }

    /**
     * Returns a list of the {@link OptionalViewEditFunction}s that the panel supports. By default,
     * none are provided.
     */
    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return Set.of();
    }

    private JMenuItem menuItem(OptionalViewEditFunction function) {
        return createMenuItem(function.toString() + DIALOG, getActionByFunction().get(function));
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

    private void toggleExpansionState(Outline outline, java.awt.event.MouseEvent evt) {
        TreePath treePath = outline.getClosestPathForLocation(evt.getPoint().x, evt.getPoint().y);
        if (outline.isExpanded(treePath)) {
            outline.collapsePath(treePath);
        } else {
            outline.expandPath(treePath);
        }
    }

    private void selectAndEditRow(MouseEvent evt) {
        int tableRow = table.rowAtPoint(evt.getPoint());
        selectedModelRow = table.convertRowIndexToModel(tableRow);
        if (evt.getClickCount() == 2) {
            try {
                if (ziggyTable.getContentAtViewRow(selectedModelRow) != null) {
                    edit(selectedModelRow);
                }
            } catch (Exception e) {
                MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    protected abstract void refresh();

    protected void view(int row) {
    }

    protected abstract void edit(int row);

    /**
     * Returns a map of {@link Action} by {@link OptionalViewEditFunction}, which can be used to
     * enable or disable buttons or menu items, for example.
     */
    private Map<OptionalViewEditFunction, Action> getActionByFunction() {
        return actionByFunction;
    }
}

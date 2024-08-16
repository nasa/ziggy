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
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.table.TableModel;

import org.netbeans.swing.etable.ETable;
import org.netbeans.swing.outline.RowModel;

import gov.nasa.ziggy.pipeline.definition.UniqueNameVersionPipelineComponent;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.ZiggyTreeModel;
import gov.nasa.ziggy.util.ZiggyStringUtils;

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
        REFRESH, NEW, VIEW, EDIT, COPY, RENAME, DELETE;

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
        ziggyTable = new ZiggyTable<>(tableModel);
        table = getTable(ziggyTable);
    }

    public AbstractViewEditPanel(RowModel rowModel, ZiggyTreeModel<?> treeModel,
        String nodesColumnLabel) {
        ziggyTable = new ZiggyTable<>(rowModel, treeModel, nodesColumnLabel);
        table = getTable(ziggyTable);
    }

    /**
     * Obtain the table from the {@code ZiggyTable} and set some general things.
     * <p>
     * In particular, tables are set up with {@link ListSelectionModel#SINGLE_SELECTION} since
     * almost none of the Ziggy tables are set up to work with multiple selections. The selection
     * model can be updated if necessary.
     */
    private ETable getTable(ZiggyTable<T> ziggyTable) {
        table = ziggyTable.getTable();
        table.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        return table;
    }

    protected void buildComponent() {
        setLayout(new BorderLayout());
        setPreferredSize(new Dimension(400, 300));
        add(getButtonPanel(), BorderLayout.NORTH);
        add(getScrollPane(), BorderLayout.CENTER);
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

        panelActions.put(OptionalViewEditFunction.NEW, new AbstractAction(NEW, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    create();
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

        panelActions.put(OptionalViewEditFunction.COPY, new AbstractAction(COPY, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    copy(selectedModelRow);
                } catch (Exception e) {
                    MessageUtils
                        .showError(SwingUtilities.getWindowAncestor(AbstractViewEditPanel.this), e);
                }
            }
        });

        panelActions.put(OptionalViewEditFunction.RENAME, new AbstractAction(RENAME, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    rename(selectedModelRow);
                } catch (Exception e) {
                    MessageUtils
                        .showError(SwingUtilities.getWindowAncestor(AbstractViewEditPanel.this), e);
                }
            }
        });

        panelActions.put(OptionalViewEditFunction.DELETE, new AbstractAction(DELETE, null) {
            @Override
            public void actionPerformed(ActionEvent evt) {
                try {
                    delete(selectedModelRow);
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
                createButton(getActionByFunction().get(OptionalViewEditFunction.REFRESH)),
                optionalViewEditFunctions().contains(OptionalViewEditFunction.NEW)
                    ? createButton(getActionByFunction().get(OptionalViewEditFunction.NEW))
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
                MessageUtils.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    /**
     * Set the popup menu on the table.
     */
    private void setComponentPopupMenu(final Component parent, final JPopupMenu menu) {

        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    ZiggySwingUtils.adjustSelection(table, e);
                    selectedModelRow = table.convertRowIndexToModel(table.getSelectedRow());
                    T contentAtViewRow = ziggyTable.getContentAtViewRow(selectedModelRow);
                    if (contentAtViewRow != null) {
                        if (contentAtViewRow instanceof UniqueNameVersionPipelineComponent) {
                            disableActionsWhenInUse(contentAtViewRow);
                        }
                        menu.show(parent, e.getX(), e.getY());
                    }
                }
            }

            private void disableActionsWhenInUse(T contentAtViewRow) {
                UniqueNameVersionPipelineComponent<?> pipelineComponent = (UniqueNameVersionPipelineComponent<?>) contentAtViewRow;
                boolean inUse = pipelineComponent.getVersion() > 0 || pipelineComponent.isLocked();
                updateAction(OptionalViewEditFunction.RENAME, inUse);
                updateAction(OptionalViewEditFunction.DELETE, inUse);
            }

            private void updateAction(OptionalViewEditFunction function, boolean locked) {
                Action action = getActionByFunction().get(function);
                action.setEnabled(!locked);

                // If we have a function that needs to end in "ed", then we'll do something
                // different.
                action.putValue(Action.SHORT_DESCRIPTION,
                    locked
                        ? "Components used in a pipeline run can not be "
                            + function.toString().toLowerCase() + "d"
                        : "");
            }
        });
    }

    private JPopupMenu getPopupMenu() {

        JPopupMenu popupMenu = new JPopupMenu();
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.NEW,
            menuItem(OptionalViewEditFunction.NEW));
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.VIEW,
            menuItem(OptionalViewEditFunction.VIEW));
        popupMenu.add(menuItem(OptionalViewEditFunction.EDIT));
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.COPY,
            menuItem(OptionalViewEditFunction.COPY));
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.RENAME,
            menuItem(OptionalViewEditFunction.RENAME));
        addOptionalMenuItem(popupMenu, OptionalViewEditFunction.DELETE,
            menuItem(OptionalViewEditFunction.DELETE));

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

    protected Set<OptionalViewEditFunction> optionalViewEditFunctions() {
        return Set.of(OptionalViewEditFunction.DELETE, OptionalViewEditFunction.NEW);
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

    private Map<OptionalViewEditFunction, Action> getActionByFunction() {
        return actionByFunction;
    }
}

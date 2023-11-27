package gov.nasa.ziggy.ui.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.Component;
import java.awt.Container;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.GridLayout;
import java.awt.Window;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.plaf.metal.MetalLookAndFeel;

import com.jgoodies.looks.plastic.theme.SkyBluer;

import gov.nasa.ziggy.ui.util.table.TableMouseListener;

/**
 * A handful of Swing-related utilities.
 *
 * @see SwingUtilities
 * @author Bill Wohler
 */
public class ZiggySwingUtils {

    public enum ButtonPanelContext {
        /**
         * Tool bar buttons have leading alignment and maintain their order on all platforms.
         * Leading alignment is left-justified in English, for example.
         */
        TOOL_BAR,

        /**
         * Buttons at the bottom of dialog have trailing alignment and are sorted per the platform.
         * Trailing alignment is right-justified in English, for example.
         */
        // TODO Make the primary button the default and bind the secondary button to escape
        // The default button is run when Enter is pressed.
        DIALOG;
    }

    public enum LabelType {
        /** A regular label, emboldened at the default font size. */
        DEFAULT,

        /**
         * A heading for a group of fields, emboldened and a couple of points larger and the same
         * color as a titled border title so that it stands out more.
         */
        HEADING1,

        /**
         * A heading for a group of fields, emboldened and a single point larger and the same color
         * as a titled border title so that it stands out more.
         */
        HEADING2,

        /**
         * A heading for a group of fields, emboldened and the same color as a titled border title
         * so that it stands out more.
         */
        HEADING;
    }

    /** A menu item that results in a menu separator. */
    public static final JMenuItem MENU_SEPARATOR = new JMenuItem(
        JPopupMenu.Separator.class.toString());

    /** The gap between dialog groups. Use in the {@link GroupLayout} {@code addGap()} method. */
    public static final int GROUP_GAP = 20;

    /**
     * The indent of a group under its heading. Use in the {@link GroupLayout} {@code addGap()}
     * method.
     */
    public static final int INDENT = 30;

    /**
     * Minimum size for all dialogs.
     */
    public static final Dimension MIN_DIALOG_SIZE = new Dimension(500, 375);

    static {
        setLookAndFeel();
    }

    public static void setLookAndFeel() {
        try {
            MetalLookAndFeel.setCurrentTheme(new SkyBluer());
            UIManager.setLookAndFeel("com.jgoodies.looks.plastic.Plastic3DLookAndFeel");
        } catch (Exception e) {
            MessageUtil.showError(null, e);
        }
    }

    /** Returns a bold label for the given string. */
    public static JLabel boldLabel(String s) {
        return boldLabel(s, LabelType.DEFAULT);
    }

    /** Returns a bold label for the given string of the given type. */
    public static JLabel boldLabel(String s, LabelType type) {
        checkNotNull(s, "s");
        checkNotNull(type, "type");

        // If we ever want to embolden the existing style, use
        // label.getFont().getStyle() | Font.BOLD.
        JLabel label = new JLabel(s);
        switch (type) {
            case DEFAULT:
                label.setFont(label.getFont().deriveFont(Font.BOLD));
                break;
            case HEADING:
            case HEADING1:
            case HEADING2:
                int size = label.getFont().getSize();
                label.setFont(label.getFont()
                    .deriveFont(Font.BOLD, size
                        + (type == LabelType.HEADING1 ? 2 : type == LabelType.HEADING2 ? 1 : 0)));
                label.setForeground(UIManager.getColor("TitledBorder.titleColor"));
                break;
        }
        return label;
    }

    public static JMenuItem createMenuItem(String text, ActionListener listener) {
        JMenuItem menuItem = new JMenuItem(text);
        menuItem.addActionListener(listener);
        return menuItem;
    }

    /**
     * Creates a menu with the given items.
     * <p>
     * A menu item can be null to allow optional items. For example:
     *
     * <pre>
     * createMenu(option == true ? createMenuItem() : null)
     * </pre>
     *
     * A separator can be added by specifying the {@link #MENU_SEPARATOR} menu item.
     */
    public static JMenu createMenu(String text, JMenuItem... menuItems) {
        JMenu menu = new JMenu(text);
        for (JMenuItem menuItem : menuItems) {
            if (menuItem != null) {
                if (menuItem == MENU_SEPARATOR) {
                    menu.addSeparator();
                } else {
                    menu.add(menuItem);
                }
            }
        }
        return menu;
    }

    /**
     * Creates a menu with the given items.
     * <p>
     * A menu item can be null to allow optional items. For example:
     *
     * <pre>
     * createMenu(option == true ? createMenuItem() : null)
     * </pre>
     *
     * A separator can be added by specifying the {@link #MENU_SEPARATOR} menu item.
     */
    public static JPopupMenu createPopupMenu(JMenuItem... menuItems) {
        JPopupMenu menu = new JPopupMenu();
        for (JMenuItem menuItem : menuItems) {
            if (menuItem != null) {
                if (menuItem == MENU_SEPARATOR) {
                    menu.addSeparator();
                } else {
                    menu.add(menuItem);
                }
            }
        }
        return menu;
    }

    /**
     * Adds the specified table mouse listener to receive mouse events from the given table. If
     * {@code table} or {@code tableMouseListener} are null, no exception is thrown and no action is
     * performed.
     *
     * @param menu if non-null, displayed if a context menu is requested
     */

    public static void addTableMouseListener(JTable table, JPopupMenu menu,
        TableMouseListener tableMouseListener) {

        if (table == null || tableMouseListener == null) {
            return;
        }

        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent evt) {
                try {
                    tableMouseListener.rowSelected(table.rowAtPoint(evt.getPoint()));
                    if (evt.isPopupTrigger()) {
                        if (menu != null) {
                            menu.show(table, evt.getX(), evt.getY());
                        }
                    } else if (evt.getClickCount() == 2) {
                        tableMouseListener.rowDoubleClicked(table.rowAtPoint(evt.getPoint()));
                    }
                } catch (Exception e) {
                    MessageUtil.showError(SwingUtilities.getWindowAncestor(table), e);
                }
            }
        });
    }

    public static JButton createButton(String text, ActionListener listener) {
        return createButton(text, null, listener);
    }

    public static JButton createButton(String text, String toolTipText,
        ActionListener actionListener) {

        checkNotNull(text, "text");
        // toolTipText is ignored if null.
        checkNotNull(actionListener, "actionListener");
        JButton button = new JButton(text);
        if (toolTipText != null) {
            button.setToolTipText(toolTipText);
        }
        button.addActionListener(actionListener);
        return button;
    }

    /**
     * Creates a standard right-aligned button panel with the given buttons. A button can be null to
     * allow optional buttons. For example:
     *
     * <pre>
     * createButtonPanel(option == true ? createButton() : null)
     * </pre>
     *
     * Primary buttons that initiates the dialog's action should be listed first followed by
     * secondary buttons such as Close or Cancel. The buttons will actually be laid out per the
     * platform-specified order. For example, for the statement
     * {@code createButtonPanel(OK, CANCEL)}, the button order will be Cancel/OK on the Mac and
     * Linux and Android and OK/Cancel on Windows.
     */
    public static JPanel createButtonPanel(JButton... buttons) {
        checkNotNull(buttons, "buttons");

        return createButtonPanel(ButtonPanelContext.DIALOG, buttons);
    }

    /**
     * Creates a standard button panel with the given buttons. The button layout is controlled by
     * the context. The context should be {@link ButtonPanelContext#TOOL_BAR} for buttons at the top
     * of a panel and {@link ButtonPanelContext#DIALOG} for buttons at the bottom of the panel, or
     * just use {@link #createButtonPanel(JButton...)}.
     * <p>
     * A button can be null to allow optional buttons. For example:
     *
     * <pre>
     * createButtonPanel(ButtonPanelContext.TOOL_BAR, option == true ? createButton() : null)
     * </pre>
     *
     * Primary buttons that initiates the dialog's action should be listed first followed by
     * secondary buttons such as Close or Cancel. The buttons will actually be laid out per the
     * platform-specified order. For example, for the statement
     * {@code createButtonPanel(OK, CANCEL)}, the button order will be Cancel/OK on the Mac and
     * Linux and Android and OK/Cancel on Windows.
     *
     * @see ButtonPanelContext
     */
    public static JPanel createButtonPanel(ButtonPanelContext context, JButton... buttons) {
        checkNotNull(context, "context");
        checkNotNull(buttons, "buttons");

        JButton[] platformOrderedButtons = buttons;
        if (context == ButtonPanelContext.DIALOG
            && UIManager.getDefaults().getBoolean("OptionPane.isYesLast")) {
            Collections.reverse(Arrays.asList(platformOrderedButtons));
        }

        // Lay out one row of equally-sized buttons with a horizontal gap of 5 pixels.
        JPanel buttonGrid = new JPanel(new GridLayout(1, 0, 5, 0));
        for (JButton button : platformOrderedButtons) {
            if (button != null) {
                buttonGrid.add(button);
            }
        }

        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new FlowLayout(
            context == ButtonPanelContext.DIALOG ? FlowLayout.TRAILING : FlowLayout.LEADING));
        buttonPanel.add(buttonGrid);

        return buttonPanel;
    }

    /**
     * Adds buttons to a button panel created with {@link #createButtonPanel(JButton...)}. Do not
     * use this method to add buttons at the bottom of the dialog as the results are undefined.
     */
    public static void addButtonsToPanel(JPanel panel, JButton... buttons) {
        JPanel buttonGrid = (JPanel) panel.getComponent(0);
        for (JButton button : buttons) {
            if (button != null) {
                buttonGrid.add(button);
            }
        }
    }

    /**
     * Prepends buttons to a button panel created with {@link #createButtonPanel(JButton...)}. Do
     * not use this method to add buttons at the bottom of the dialog as the results are undefined.
     */
    public static void prependButtonsToPanel(JPanel panel, JButton... buttons) {
        JPanel buttonGrid = (JPanel) panel.getComponent(0);
        int i = 0;
        for (JButton button : buttons) {
            if (button != null) {
                buttonGrid.add(button, i++);
            }
        }
    }

    /**
     * Returns the dialog for the given component
     *
     * @param component the component
     * @return the component's dialog, or null if the component isn't a descendent of a dialog
     */
    public static Dialog getDialog(Component component) {
        for (Container c = component.getParent(); c != null; c = c.getParent()) {
            if (c instanceof JDialog) {
                return (JDialog) c;
            }
        }
        return null;
    }

    /**
     * Determines the width of the given string for the given component in pixels.
     *
     * @param component a {@link JComponent}
     * @param s the string
     * @return the width of the string, in pixels, or 0 if {@code component} or {@code s} is null or
     * empty
     */
    public static int textWidth(JComponent component, String s) {
        if (component == null || s == null || s.length() == 0) {
            return 0;
        }

        FontMetrics metrics = component.getFontMetrics(component.getFont());

        return metrics.stringWidth(s);
    }

    /**
     * Determines the height of the given string for the given component in pixels.
     *
     * @param component a {@link JComponent}
     * @return the height of the string, in pixels, or 0 if {@code component} is null or empty
     */
    public static int textHeight(JComponent component) {
        if (component == null) {
            return 0;
        }

        FontMetrics metrics = component.getFontMetrics(component.getFont());

        return metrics.getHeight();
    }

    /**
     * Displays any non-null component. JFrames and JDialogs are displayed as-is. JPanels and other
     * Containers are displayed within a JFrame.
     * <p>
     * N.B. This is intended to be called by a temporary main() during development as it calls
     * System.exit(0) when the window is closed or even hidden to avoid a slew of orphaned Java
     * processes
     */
    public static void displayTestDialog(Component component) {
        checkNotNull(component, "component");

        SwingUtilities.invokeLater(() -> {
            Window window = component instanceof Window ? (Window) component
                : SwingUtilities.getWindowAncestor(component);

            // Enclose panel in a JFrame.
            if (window == null) {
                JFrame frame = new JFrame();
                frame.getContentPane().add(component);
                window = frame;
            }

            // Ensure JVM exits when we're done.
            window.addWindowListener(new WindowAdapter() {
                // Called when the window is closed with an X button.
                @Override
                public void windowClosing(WindowEvent e) {
                    System.exit(0);
                }

                // Called when the window is no longer visible. This can be explicit when calling
                // setVisible(false) upon an OK or Cancel button press, or implicit when the window
                // is covered by another. This is fine for development.
                @Override
                public void windowDeactivated(WindowEvent e) {
                    System.exit(0);
                }
            });

            // Display window.
            window.pack();
            window.setLocationRelativeTo(component);
            window.setVisible(true);
        });
    }

    /**
     * Display the current UI defaults and their current value. Use {@link UIManager} to get these
     * values. For example:
     *
     * <pre>
     * UIManager.getBorder("TextField.border")
     * </pre>
     * <p>
     * Thanks to https://www.logicbig.com/tutorials/java-swing/ui-default.html.
     */
    public static void main(String[] args) throws Exception {
        List<Map.Entry<Object, Object>> entries = new ArrayList<>(
            UIManager.getDefaults().entrySet());
        Collections.sort(entries, Comparator.comparing(e -> Objects.toString(e.getKey())));
        entries.forEach(ZiggySwingUtils::printUiDefaultsEntry);
    }

    private static void printUiDefaultsEntry(Map.Entry<Object, Object> e) {
        System.out.printf("%-53s= %s%n", e.getKey(), e.getValue()); // .getClass().getTypeName()
    }
}

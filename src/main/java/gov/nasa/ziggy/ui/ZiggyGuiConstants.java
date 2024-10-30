package gov.nasa.ziggy.ui;

import java.awt.Dimension;

import javax.swing.GroupLayout;

/**
 * General constants used by the Ziggy Console.
 * <p>
 * Ziggy uses sentence capitalization for nearly all elements in menu items, button text, labels,
 * table headings.
 */
public class ZiggyGuiConstants {

    /** Minimum width to display instances and tasks optimally. */
    public static final int MAIN_WINDOW_WIDTH = 1500;

    /**
     * Desired aspect ratio of application. The ratio 8:5 is a common laptop and tablet ratio and is
     * close to the golden ratio.
     */
    public static final float MAIN_WINDOW_ASPECT_RATIO = (float) (8.0 / 5.0);

    /** Minimum size for all but the smallest of dialogs. */
    public static final Dimension MIN_DIALOG_SIZE = new Dimension(500,
        (int) (500.0 / MAIN_WINDOW_ASPECT_RATIO));

    /** Minimum size for small dialogs. */
    public static final Dimension MIN_SMALL_DIALOG_SIZE = new Dimension(300,
        (int) (300.0 / MAIN_WINDOW_ASPECT_RATIO));

    /**
     * The size of the gap between the container and components that touch the border of the
     * container. In most cases, {@link GroupLayout#setAutoCreateContainerGaps(boolean)} adds a
     * little breathing room around a panel.
     */
    public static final int CONTAINER_GAP = 5;

    /** The gap between dialog groups. Use in the {@link GroupLayout} {@code addGap()} method. */
    public static final int GROUP_GAP = 20;

    /**
     * The indent of a group under its heading. Use in the {@link GroupLayout} {@code addGap()}
     * method.
     */
    public static final int INDENT = 30;

    /**
     * The number of preferred rows in a table. Usually used to ensure the table is of a minimum
     * size before data has been loaded into it.
     */
    public static final int PREFERRED_ROW_COUNT = 7;

    /** String to display on GUI while loading data in background thread. */
    public static final String LOADING = "Loading...";

    // Button and menu item strings.
    public static final String ABOUT = "About";
    public static final String ASSIGN_GROUP = "Assign group";
    public static final String CANCEL = "Cancel";
    public static final String CLOSE = "Close";
    public static final String COLLAPSE_ALL = "Collapse all";
    public static final String CREATE = "Create";
    public static final String DELETE_SYMBOL = "-";
    public static final String DIALOG = "...";
    public static final String EDIT = "Edit";
    public static final String EXIT = "Exit";
    public static final String EXPAND_ALL = "Expand all";
    public static final String EXPORT = "Export";
    public static final String FILE = "File";
    public static final String HELP = "Help";
    public static final String IMPORT = "Import";
    public static final String NEW_SYMBOL = "+";
    public static final String OK = "OK";
    public static final String REFRESH = "Refresh";
    public static final String REPORT = "Report";
    public static final String RESTART = "Restart";
    public static final String RESTORE_DEFAULTS = "Restore defaults";
    public static final String SAVE = "Save";
    public static final String SELECT = "Select";
    public static final String START = "Start";
    public static final String TO_BOTTOM = "To bottom";
    public static final String TO_TOP = "To top";
    public static final String VIEW = "View";
}

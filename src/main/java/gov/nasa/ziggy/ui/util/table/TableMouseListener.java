package gov.nasa.ziggy.ui.util.table;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * The listener interface for receiving "interesting" mouse events on a table.
 * <p>
 * The class that is interested in processing a table event implements this interface. The listener
 * object created from that class is then registered with a table by using the
 * {@link ZiggySwingUtils#addTableMouseListener(javax.swing.JTable, javax.swing.JPopupMenu, TableMouseListener)}
 * method. An event is generated when the mouse is pressed. When a mouse event occurs, the relevant
 * method in the listener object is invoked, and the row associated with the mouse's location is
 * passed to it.
 */
public interface TableMouseListener {

    /**
     * Invoked when a mouse button has been pressed. Clients can save the selected row and use it
     * when a menu command is run, for example.
     *
     * @param row the row under the mouse
     */
    void rowSelected(int row);

    /**
     * Invoked when the mouse button has been double-clicked on a table.
     *
     * @param row the row under the mouse
     */
    void rowDoubleClicked(int row);
}

/**
 * Provides classes and interfaces for the Ziggy graphical user interface (GUI) and command-line
 * interface (CLI). The rest of this page contains general conventions for developing code in the
 * gov.nasa.ziggy.ui package.
 * <h2>Database access</h2>
 * <p>
 * Use an operations class rather than a CRUD class directly. The operations class performs each
 * query in a separate session and transaction.
 * <p>
 * Calling an operations method should be performed in a {@link javax.swing.SwingWorker} in order to
 * keep a potentially slow call off the event dispatch thread (EDT) as this can cause the UI to
 * freeze and leave gray blocks if dragging windows in front of it. There are two approaches that
 * can be taken depending on whether the data is needed to size the panel or dialog or not.
 * <p>
 * The size of the dialog is realized when the {@code pack()} is called. It is important to call
 * this method before calling {@code setPositionRelativeTo()} so the latter can center the dialog in
 * the parent window. It is also important to ensure that {@code pack()} operates on either a
 * populated panel, or one whose components have been given preferred or minimum sizes.
 * <dl>
 * <dt>Data is not needed to size panel
 * <dd>In this scenario, the size of the panel can be determined without having the information from
 * the database.
 * <p>
 * Create the panel using the text {@link gov.nasa.ziggy.ui.ZiggyGuiConstants#LOADING} as a
 * placeholder for content that will come from the database. After the {@code buildComponent()} and
 * {@code setPositionRelativeTo()} calls in the constructor, call {@code loadFromDatabase()} (by
 * convention), which should contain a {@link javax.swing.SwingWorker} to perform the database call
 * and update the panel.
 * <p>
 * This method has the advantage of bringing up the dialog fast.
 * <dt>Data is needed to size panel
 * <dd>In this scenario, the panel contains a list of items and it would be nicer to shrink-wrap the
 * dialog around the list. Thus, the items have to be loaded before the the panel can be realized.
 * <p>
 * In this case, create a static factory method that contains a {@link javax.swing.SwingWorker}.
 * Perform the database lookup in the {@code doInBackground()} method, and then create the dialog
 * and call the {@link java.awt.Window#setVisible(boolean)} in the {@code done()} method.
 * <p>
 * This method has the disadvantage that there is a lag before the dialog is shown, but this lag is
 * generally imperceptible on today's iron. The advantages are that the database operations do not
 * occur on the EDT and that the dialog is sized nicely around its content.
 * </dl>
 * <p>
 * If a {@code doInBackground()} method needs to obtain user input, use
 * {@link javax.swing.SwingUtilities#invokeAndWait(Runnable)}.
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.ui;

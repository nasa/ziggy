package gov.nasa.ziggy.ui.common;

import java.awt.Component;

import javax.swing.JOptionPane;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * Implements common error message display utilities.
 */
public class MessageUtil {

    private static final Logger log = LoggerFactory.getLogger(ZiggyGuiConsole.class);

    /**
     * Displays an error message in a dialog. Wraps the message using HTML if the message is long.
     *
     * @param parent the parent component for the message dialog
     * @param msg the error message to display
     */
    public static void showError(Component parent, String msg) {
        // If the message is long, enclose it in some HTML that will use word-wrap.
        // Also increase the line height for readability.
        if (msg.length() >= 65) {
            msg = "<html><p style='width: 400px; line-height: 140%;'>"
                + StringEscapeUtils.escapeHtml4(msg) + "</p></html>";
        }

        JOptionPane.showMessageDialog(parent, msg, "Error", JOptionPane.ERROR_MESSAGE);
    }

    /**
     * Displays an error message for an exception in a dialog, and logs the exception.
     *
     * @param parent the parent component for the message dialog
     * @param e the exception to display
     */
    public static void showError(Component parent, Throwable e) {
        log.warn("Unexpected exception", e);
        showError(parent, e.toString());
    }

}

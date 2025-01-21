package gov.nasa.ziggy.ui.util;

import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;

import java.awt.Component;

import javax.swing.JOptionPane;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * Implements common error message display utilities.
 */
public class MessageUtils {

    private static final Logger log = LoggerFactory.getLogger(ZiggyGuiConsole.class);

    /**
     * Displays an error message in a dialog. Wraps the message using HTML if the message is long.
     *
     * @param parent the parent component for the message dialog; if null, message goes to stdout
     * @param msg the error message to display
     */
    public static void showError(Component parent, String msg) {
        showError(parent, null, msg, null);
    }

    /**
     * Displays an error message in a dialog and logs the exception. Wraps the message using HTML if
     * the message is long.
     *
     * @param parent the parent component for the message dialog; if null, message goes to stdout
     * @param msg the error message to display
     * @param e the exception to display
     */
    public static void showError(Component parent, String msg, Throwable e) {
        showError(parent, null, msg, e);
    }

    /**
     * Displays an error message for an exception in a dialog, and logs the exception.
     *
     * @param parent the parent component for the message dialog; if null, message goes to stdout
     * @param e the exception to display
     */
    public static void showError(Component parent, Throwable e) {
        showError(parent, null, e.toString(), e);
    }

    /**
     * Displays an error message in a dialog. Wraps the message using HTML if the message is long.
     *
     * @param parent the parent component for the message dialog; if null, message goes to stdout
     * @param title the title for the message dialog
     * @param msg the error message to display
     * @param e the exception to log
     */
    public static void showError(Component parent, String title, String msg, Throwable e) {
        String logFormat = "title={}, msg={}";
        if (e == null) {
            log.warn(logFormat, title, msg);
        } else {
            log.warn(logFormat, title, msg, e);
        }

        if (parent == null) {
            System.out.println(msg);
            return;
        }

        JOptionPane.showMessageDialog(parent, wordWrap(msg), title != null ? title : "Error",
            JOptionPane.ERROR_MESSAGE);
    }

    /**
     * Displays an informational message in a dialog. Wraps the message using HTML if the message is
     * long.
     *
     * @param parent the parent component for the message dialog; if null, message goes to stdout
     * @param title the title for the message dialog
     * @param msg the message to display
     */
    public static void showInfo(Component parent, String title, String msg) {
        if (parent == null) {
            System.out.println(msg);
            return;
        }

        JOptionPane.showMessageDialog(parent, wordWrap(msg), title != null ? title : "Information",
            JOptionPane.INFORMATION_MESSAGE);
    }

    private static String wordWrap(String msg) {
        // If the message is long, enclose it in some HTML that will use word-wrap.
        // Also increase the line height for readability.
        if (msg.length() >= 65) {
            msg = htmlBuilder()
                .appendCustomHtml("<p style='width: 400px; line-height: 140%;'>",
                    StringEscapeUtils.escapeHtml4(msg), "</p>")
                .toString();
        }
        return msg;
    }
}

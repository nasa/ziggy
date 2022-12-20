package gov.nasa.ziggy.ui.common;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Display a String message in a scrollable JTextArea
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class MessageDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(MessageDialog.class);

    private JPanel dataPanel;
    private JTextArea messageTextArea;
    private JScrollPane textAreaScrollPane;
    private JButton closeButton;
    private JPanel buttonPanel;

    private String message = "";

    public MessageDialog(JFrame parent, String message) {
        super(parent, true);
        this.message = message;

        initGUI();
    }

    public MessageDialog(JDialog parent, String message) {
        super(parent, true);
        this.message = message;

        initGUI();
    }

    public static void showMessageDialog(JFrame parent, String message) {
        MessageDialog d = new MessageDialog(parent, message);
        d.setVisible(true);
    }

    public static void showMessageDialog(JDialog parent, String message) {
        MessageDialog d = new MessageDialog(parent, message);
        d.setVisible(true);
    }

    private void closeButtonActionPerformed(ActionEvent evt) {
        log.debug("closeButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void initGUI() {
        try {
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            setSize(400, 300);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getTextAreaScrollPane(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            buttonPanel.add(getCloseButton());
        }
        return buttonPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(this::closeButtonActionPerformed);
        }
        return closeButton;
    }

    private JScrollPane getTextAreaScrollPane() {
        if (textAreaScrollPane == null) {
            textAreaScrollPane = new JScrollPane();
            textAreaScrollPane.setViewportView(getMessageTextArea());
        }
        return textAreaScrollPane;
    }

    private JTextArea getMessageTextArea() {
        if (messageTextArea == null) {
            messageTextArea = new JTextArea(message);
        }
        return messageTextArea;
    }
}

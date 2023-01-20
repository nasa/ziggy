package gov.nasa.ziggy.ui;

import java.awt.BorderLayout;
import java.awt.GridLayout;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import gov.nasa.ziggy.util.ZiggyVersion;

/**
 * About dialog for the console
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class AboutDialog extends javax.swing.JDialog {

    public AboutDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    private void initGUI() {
        setSize(500, 160);
        getContentPane().add(contentPanel(), BorderLayout.CENTER);
        getContentPane().add(buttonPanel(), BorderLayout.SOUTH);

        setTitle("About Ziggy");
    }

    private JPanel contentPanel() {

        JPanel contentPanel = new JPanel();
        contentPanel.setLayout(new GridLayout(3, 2));
        contentPanel.add(new JLabel("Software version: ", SwingConstants.RIGHT));
        contentPanel.add(new JLabel(ZiggyVersion.getSoftwareVersion()));
        contentPanel.add(new JLabel("Build date: ", SwingConstants.RIGHT));
        contentPanel.add(new JLabel(ZiggyVersion.getBuildDate().toString()));
        contentPanel.add(new JLabel("URI: ", SwingConstants.RIGHT));
        contentPanel.add(new JLabel("github.com/nasa/ziggy"));

        return contentPanel;
    }

    private JPanel buttonPanel() {
        JButton closeButton = new JButton("OK");
        closeButton.addActionListener(event -> {
            setVisible(false);
            dispose();
        });

        JPanel buttonPanel = new JPanel();
        buttonPanel.add(closeButton);
        return buttonPanel;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            AboutDialog dialog = new AboutDialog(frame);
            dialog.setVisible(true);
            frame.dispose();
        });
    }
}

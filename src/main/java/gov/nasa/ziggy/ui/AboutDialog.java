package gov.nasa.ziggy.ui;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.OK;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.Window;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * About dialog for the console
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class AboutDialog extends javax.swing.JDialog {

    public AboutDialog(Window owner) {
        super(owner, DEFAULT_MODALITY_TYPE);
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setSize(500, 160);
        getContentPane().add(contentPanel(), BorderLayout.CENTER);
        getContentPane().add(buttonPanel(), BorderLayout.SOUTH);

        setTitle("About Ziggy");
    }

    private JPanel contentPanel() {

        JPanel contentPanel = new JPanel();
        contentPanel.setLayout(new GridLayout(3, 2));
        contentPanel.add(new JLabel("Software version: ", SwingConstants.RIGHT));
        contentPanel.add(new JLabel(
            ZiggyConfiguration.getInstance().getString(PropertyName.ZIGGY_VERSION.property())));
        contentPanel.add(new JLabel("URI: ", SwingConstants.RIGHT));
        contentPanel.add(new JLabel("github.com/nasa/ziggy"));

        return contentPanel;
    }

    private JPanel buttonPanel() {
        JButton closeButton = new JButton(OK);
        closeButton.addActionListener(event -> {
            setVisible(false);
            dispose();
        });

        JPanel buttonPanel = new JPanel();
        buttonPanel.add(closeButton);
        return buttonPanel;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new AboutDialog(null));
    }
}

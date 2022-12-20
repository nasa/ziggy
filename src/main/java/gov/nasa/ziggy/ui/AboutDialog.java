package gov.nasa.ziggy.ui;

import java.awt.BorderLayout;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import com.jgoodies.forms.builder.DefaultFormBuilder;
import com.jgoodies.forms.factories.Borders;
import com.jgoodies.forms.layout.FormLayout;

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
        FormLayout layout = new FormLayout("right:pref, 3dlu, default:grow", "");
        DefaultFormBuilder builder = new DefaultFormBuilder(layout).border(Borders.DIALOG)
            .rowGroupingEnabled(true);

        builder.append("Software Version:", getValueField(ZiggyVersion.getSoftwareVersion()));
        builder.append("Build date:", getValueField(ZiggyVersion.getBuildDate().toString()));

        getContentPane().add(builder.build(), BorderLayout.CENTER);
        getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);

        setTitle("About Ziggy");
        pack();
    }

    /**
     * Gets a component that is appropriate for showing a non-editable value.
     *
     * @param value the text value to display
     * @return a component that will display the value
     */
    private JComponent getValueField(String value) {
        return new JLabel(value);
    }

    private JPanel getButtonPanel() {
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

package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetViewDialog extends javax.swing.JDialog {
    // private static final Logger log = LoggerFactory.getLogger(ParameterSetViewDialog.class);

    private JPanel actionPanel;
    private ParameterSetViewPanel parameterSetViewPanel;
    private JButton closeButton;

    private Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap = null;

    public ParameterSetViewDialog(JDialog parentDialog) {
        this(parentDialog, null);
    }

    public ParameterSetViewDialog(JDialog parentDialog,
        Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap) {
        super(parentDialog);

        this.parameterSetsMap = parameterSetsMap;

        initGUI();
    }

    public static void showParameterSet(JDialog parentDialog,
        Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap) {
        ParameterSetViewDialog dialog = new ParameterSetViewDialog(parentDialog, parameterSetsMap);

        dialog.setVisible(true);
    }

    private void closeButtonActionPerformed() {
        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                setTitle("View Parameter Sets");
                getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
                getContentPane().add(getParameterSetViewPanel(), BorderLayout.CENTER);
            }
            setSize(400, 300);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            actionPanel.add(getCloseButton());
        }
        return actionPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(evt -> closeButtonActionPerformed());
        }
        return closeButton;
    }

    private ParameterSetViewPanel getParameterSetViewPanel() {
        if (parameterSetViewPanel == null) {
            parameterSetViewPanel = new ParameterSetViewPanel(parameterSetsMap);
        }
        return parameterSetViewPanel;
    }

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JDialog d = new JDialog();
            ParameterSetViewDialog inst = new ParameterSetViewDialog(d);
            inst.setVisible(true);
        });
    }
}

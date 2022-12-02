package gov.nasa.ziggy.ui.common;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.TestBean;

@SuppressWarnings("serial")
public class TestPropertyEditorDialog extends javax.swing.JDialog {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TestPropertyEditorDialog.class);

    private JPanel dataPanel;
    private PropertySheetPanel propertySheetPanel;
    private JButton exitButton;
    private JPanel actionPanel;
    private final TestBean currentParams = new TestBean();

    public TestPropertyEditorDialog(JFrame frame) {
        super(frame);
        initGUI();
    }

    private void initGUI() {
        try {
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            setSize(400, 300);

            populateParamsPropertySheet();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void populateParamsPropertySheet() {
        if (currentParams != null) {
            try {
                PropertySheetHelper.populatePropertySheet(currentParams, propertySheetPanel);
            } catch (Exception e) {
                throw new PipelineException("Failed to introspect Parameters bean", e);
            }
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getPropertySheetPanel(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            actionPanel.add(getExitButton());
        }
        return actionPanel;
    }

    private JButton getExitButton() {
        if (exitButton == null) {
            exitButton = new JButton();
            exitButton.setText("Vamoose!");
            exitButton.addActionListener(evt -> exitButtonActionPerformed(evt));
        }
        return exitButton;
    }

    private void exitButtonActionPerformed(ActionEvent evt) {
        System.out.println("exitButton.actionPerformed, event=" + evt);
        System.exit(1);
    }

    private PropertySheetPanel getPropertySheetPanel() {
        if (propertySheetPanel == null) {
            propertySheetPanel = new PropertySheetPanel();
        }
        return propertySheetPanel;
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            TestPropertyEditorDialog inst = new TestPropertyEditorDialog(frame);
            inst.setVisible(true);
        });
    }
}

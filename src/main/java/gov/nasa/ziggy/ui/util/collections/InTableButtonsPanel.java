package gov.nasa.ziggy.ui.util.collections;

import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JTextField;

import com.l2fprod.common.swing.ComponentFactory;
import com.l2fprod.common.swing.LookAndFeelTweaks;
import com.l2fprod.common.swing.PercentLayout;

/**
 * Use this to implement the table cell that will launch the editor.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
class InTableButtonsPanel extends JPanel {
    private final JTextField textField;
    private final JButton editButton;
    private final JButton cancelButton;

    public InTableButtonsPanel(boolean asTableEditor, ActionListener editActionListener,
        ActionListener cancelActionListener) {
        super(new PercentLayout(PercentLayout.HORIZONTAL, 3));

        textField = new JTextField();
        if (asTableEditor) {
            textField.setBorder(LookAndFeelTweaks.EMPTY_BORDER);
        }
        add(textField, "*");

        editButton = ComponentFactory.Helper.getFactory().createMiniButton();
        editButton.addActionListener(editActionListener);
        add(editButton);

        cancelButton = ComponentFactory.Helper.getFactory().createMiniButton();
        cancelButton.setText("X");
        cancelButton.addActionListener(cancelActionListener);
        add(cancelButton);
    }

    @Override
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        textField.setEnabled(enabled);
        editButton.setEnabled(enabled);
        cancelButton.setEnabled(enabled);
    }

    public void setEditable(boolean editable) {
        textField.setEditable(editable);
    }

    public String getText() {
        return textField.getText();
    }

    public void setText(String text) {
        textField.setText(text);
    }
}

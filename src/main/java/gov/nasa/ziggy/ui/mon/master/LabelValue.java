package gov.nasa.ziggy.ui.mon.master;

import java.awt.FlowLayout;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.WindowConstants;

public class LabelValue extends javax.swing.JPanel {
    private static final long serialVersionUID = -5129404649677474189L;

    private JLabel label;
    private String name = "name";
    private String value = "value";

    public LabelValue() {
        initGUI();
    }

    /**
     * @param name
     * @param value
     */
    public LabelValue(String name, String value) {
        this.name = name;
        this.value = value;
        initGUI();
    }

    private void initGUI() {
        try {
            // START >> this
            FlowLayout thisLayout = new FlowLayout();
            thisLayout.setAlignment(FlowLayout.LEFT);
            setLayout(thisLayout);
            setBackground(new java.awt.Color(255, 255, 255));
            // END << this
            this.add(getLabel());
            // setPreferredSize(new Dimension(400, 300));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JLabel getLabel() {
        if (label == null) {
            label = new JLabel();
            label.setText(labelText());
            label.setFont(new java.awt.Font("Dialog", 0, 10));
        }
        return label;
    }

    private String labelText() {
        return "<html><b>" + name + ": </b> " + value + "</html>";
        // return "<html>plain <B>Bold</B> and <I>Italic</I> Text</html>";
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new LabelValue());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    /**
     * @return Returns the name.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * @param name The name to set.
     */
    @Override
    public void setName(String name) {
        this.name = name;
        label.setText(labelText());
    }

    /**
     * @return Returns the name.
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value The value to set.
     */
    public void setValue(String value) {
        this.value = value;
        label.setText(labelText());
    }
}

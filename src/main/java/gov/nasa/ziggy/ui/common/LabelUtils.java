package gov.nasa.ziggy.ui.common;

import java.awt.Font;

import javax.swing.JLabel;

public class LabelUtils {

    public static JLabel boldLabel() {
        JLabel newLabel = new JLabel();
        Font f = newLabel.getFont();
        newLabel.setFont(f.deriveFont(f.getStyle() | Font.BOLD));
        return newLabel;

    }
}

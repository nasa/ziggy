package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;

import javax.swing.JLabel;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * Displays a name: value JLabel.
 *
 * @author PT
 * @author Bill Wohler
 */
public class LabelValue extends JLabel {
    private static final long serialVersionUID = 20230822L;

    public LabelValue(String name, String value) {
        setBackground(new java.awt.Color(255, 255, 255));
        setText(htmlBuilder().appendBold(name).appendBold(": ").append(value).toString());
        setFont(new java.awt.Font("Dialog", 0, 10));
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new LabelValue("name", "value"));
    }
}

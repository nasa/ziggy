package gov.nasa.ziggy.ui.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JFrame;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

@SuppressWarnings("serial")
public class DateTextField extends javax.swing.JPanel {
    private JTextField textField;
    private Date date = new Date();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy HH:mm:ss");

    public DateTextField() {
        initGUI();
    }

    public DateTextField(Date date) {
        this.date = date;
        initGUI();
    }

    private void initGUI() {
        try {
            this.add(getTextField());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JTextField getTextField() {
        if (textField == null) {
            textField = new JTextField();
            textField.setColumns(11);
            textField.setText(dateFormat.format(date));
        }
        return textField;
    }

    /**
     * @return Returns the date.
     * @throws ParseException
     */
    public Date getDate() throws ParseException {
        Date parsedDate = dateFormat.parse(textField.getText());
        if (parsedDate == null) {
            throw new ParseException("unable to parse (parse returned null)", 0);
        }
        return parsedDate;
    }

    /**
     * @param date The date to set.
     */
    public void setDate(Date date) {
        this.date = date;
        textField.setText(dateFormat.format(date));
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new DateTextField());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}

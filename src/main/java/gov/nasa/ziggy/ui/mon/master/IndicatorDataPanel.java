package gov.nasa.ziggy.ui.mon.master;

import java.awt.GridLayout;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.WindowConstants;

public class IndicatorDataPanel extends javax.swing.JPanel {
    private static final long serialVersionUID = -5526091679673082052L;

    private JLabel jLabel1;
    private JLabel jLabel2;
    private JLabel jLabel4;
    private JLabel jLabel3;

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new IndicatorDataPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    public IndicatorDataPanel() {
        super();
        initGUI();
    }

    private void initGUI() {
        try {
            GridLayout thisLayout = new GridLayout(3, 2);
            thisLayout.setColumns(2);
            thisLayout.setRows(3);
            setLayout(thisLayout);
            this.add(getJLabel1());
            this.add(getJLabel2());
            this.add(getJLabel3());
            this.add(getJLabel4());
            // setPreferredSize(new Dimension(400, 300));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JLabel getJLabel1() {
        if (jLabel1 == null) {
            jLabel1 = new JLabel();
            jLabel1.setText("id: 42");
            jLabel1.setFont(new java.awt.Font("Dialog", 0, 10));
        }
        return jLabel1;
    }

    private JLabel getJLabel2() {
        if (jLabel2 == null) {
            jLabel2 = new JLabel();
            jLabel2.setText("state: processing");
            jLabel2.setFont(new java.awt.Font("Dialog", 0, 10));
        }
        return jLabel2;
    }

    private JLabel getJLabel3() {
        if (jLabel3 == null) {
            jLabel3 = new JLabel();
            jLabel3.setText("tasks: 40/10/3");
            jLabel3.setFont(new java.awt.Font("Dialog", 0, 10));
        }
        return jLabel3;
    }

    private JLabel getJLabel4() {
        if (jLabel4 == null) {
            jLabel4 = new JLabel();
            jLabel4.setText("workers: 5");
            jLabel4.setFont(new java.awt.Font("Dialog", 0, 10));
        }
        return jLabel4;
    }
}

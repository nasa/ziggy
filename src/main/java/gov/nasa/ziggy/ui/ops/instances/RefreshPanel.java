package gov.nasa.ziggy.ui.ops.instances;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JPanel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class RefreshPanel extends javax.swing.JPanel {
    private JButton nowButton;
    private JPanel nowButtonPanel;

    private RefreshPanelListener listener = null;

    public RefreshPanel() {
        initGUI();
    }

    private void initGUI() {
        try {
            // START >> this

            setBorder(BorderFactory.createTitledBorder("Refresh"));
            GridBagLayout thisLayout = new GridBagLayout();
            thisLayout.rowWeights = new double[] { 0.1 };
            thisLayout.rowHeights = new int[] { 7 };
            thisLayout.columnWeights = new double[] { 0.1 };
            thisLayout.columnWidths = new int[] { 7 };
            setLayout(thisLayout); // END << this
            setPreferredSize(new java.awt.Dimension(126, 72));
            this.add(getNowButtonPanel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JButton getNowButton() {
        if (nowButton == null) {
            nowButton = new JButton();
            nowButton.setText("Now");
            nowButton.addActionListener(evt -> nowButtonActionPerformed());
        }
        return nowButton;
    }

    private JPanel getNowButtonPanel() {
        if (nowButtonPanel == null) {
            nowButtonPanel = new JPanel();
            FlowLayout nowButtonPanelLayout = new FlowLayout();
            nowButtonPanelLayout.setVgap(1);
            nowButtonPanelLayout.setHgap(1);
            nowButtonPanel.setLayout(nowButtonPanelLayout);
            nowButtonPanel.add(getNowButton());
        }
        return nowButtonPanel;
    }

    /**
     * @return Returns the listener.
     */
    public RefreshPanelListener getListener() {
        return listener;
    }

    /**
     * @param listener The listener to set.
     */
    public void setListener(RefreshPanelListener listener) {
        this.listener = listener;
    }

    private void nowButtonActionPerformed() {
        if (listener != null) {
            listener.refreshNowPressed();
        }
    }
}

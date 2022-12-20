package gov.nasa.ziggy.ui.ops.instances;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TasksControlPanel extends javax.swing.JPanel implements RefreshPanelListener {
    private static final Logger log = LoggerFactory.getLogger(TasksControlPanel.class);

    private OpsInstancesPanel listener = null;
    private JButton refreshButton;

    public TasksControlPanel() {
        initGUI();
    }

    private void refreshButtonActionPerformed(ActionEvent evt) {
        log.debug("refreshButton.actionPerformed, event=" + evt);

        refreshNowPressed();
    }

    private void initGUI() {
        try {
            FlowLayout thisLayout = new FlowLayout();
            setLayout(thisLayout);
            this.add(getRefreshButton());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return Returns the listener.
     */
    public OpsInstancesPanel getListener() {
        return listener;
    }

    /**
     * @param listener The listener to set.
     */
    public void setListener(OpsInstancesPanel listener) {
        this.listener = listener;
    }

    @Override
    public void refreshNowPressed() {
        if (listener != null) {
            listener.refreshTaskNowPressed();
        }
    }

    private JButton getRefreshButton() {
        if (refreshButton == null) {
            refreshButton = new JButton();
            refreshButton.setText("refresh");
            refreshButton.addActionListener(this::refreshButtonActionPerformed);
        }
        return refreshButton;
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new TasksControlPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}

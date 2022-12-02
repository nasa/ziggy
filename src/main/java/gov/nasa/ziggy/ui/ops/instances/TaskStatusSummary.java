package gov.nasa.ziggy.ui.ops.instances;

import javax.swing.JLabel;
import javax.swing.SwingConstants;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskStatusSummary extends JLabel {
    public TaskStatusSummary() {
        setHorizontalAlignment(SwingConstants.CENTER);
        update(0, 0, 0, 0);
    }

    public void update(int submittedCount, int processingCount, int errorCount,
        int completedCount) {
        setText("<html><b><font color=green>SUBMITTED: </font></b>" + submittedCount + "      "
            + "<b><font color=green>PROCESSING: </font></b>" + processingCount + "      "
            + "<b><font color=red>FAILED: </font></b>" + errorCount + "      "
            + "<b><font color=green>COMPLETED: </font></b>" + completedCount + "</html>");
    }
}

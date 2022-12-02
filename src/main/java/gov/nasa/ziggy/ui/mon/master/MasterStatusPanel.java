package gov.nasa.ziggy.ui.mon.master;

import java.awt.BorderLayout;
import java.awt.CardLayout;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.WindowConstants;

import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.mon.alerts.AlertMessageTableModel;
import gov.nasa.ziggy.ui.mon.alerts.MonitoringAlertsPanel;
import gov.nasa.ziggy.ui.mon.master.Indicator.IdiotLight;

/**
 * This class provides a color-coded display that provides real-time status of pipeline elements,
 * including active pipelines, pipeline processes, worker threads, metrics, and alerts.
 *
 * @author Todd Klaus
 */
public class MasterStatusPanel extends javax.swing.JPanel implements IndicatorListener {
    private static final long serialVersionUID = 20220510L;

    private static MasterStatusPanel instance;

    private JSplitPane splitPane;
    private JPanel detailedPanel;
    private IndicatorPanel parentIndicatorPanel;
    private Indicator workersIndicator;
    private Indicator instancesIndicator;
    private Indicator processesIndicator;
    private Indicator alertsIndicator;
    private CardLayout detailedPanelLayout;
    private JScrollPane parentScrollPane;
    private JScrollPane detailedScrollPane;

    private WorkerStatusPanel workersPanel;
    private MonitoringAlertsPanel alertsPanel;
    private ProcessesIndicatorPanel processesPanel;

    public MasterStatusPanel() {
        initGUI();
    }

    private void initGUI() {
        try {
            setLayout(new BorderLayout());
            this.add(getSplitPane(), BorderLayout.CENTER);
            splitPane.setDividerLocation(0.35);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JSplitPane getSplitPane() {
        if (splitPane == null) {
            splitPane = new JSplitPane();
            splitPane.add(getParentScrollPane(), JSplitPane.LEFT);
            splitPane.add(getDetailedScrollPane(), JSplitPane.RIGHT);
        }
        return splitPane;
    }

    private JScrollPane getParentScrollPane() {
        if (parentScrollPane == null) {
            parentScrollPane = new JScrollPane();
            parentScrollPane.setViewportView(getParentIndicatorPanel());
        }
        return parentScrollPane;
    }

    private JScrollPane getDetailedScrollPane() {
        if (detailedScrollPane == null) {
            detailedScrollPane = new JScrollPane();
            detailedScrollPane.setViewportView(getDetailedPanel());
        }
        return detailedScrollPane;
    }

    private Indicator newIndicator(IndicatorPanel parent, String name, String greenStateToolTipText,
        String grayStateToolTipText) {
        Indicator indicator = new Indicator(parent, name);
        indicator.setIdiotLight(new IdiotLight());
        indicator.addIndicatorListener(this);
        indicator.setGreenStateToolTipText(greenStateToolTipText);
        indicator.setGrayStateToolTipText(grayStateToolTipText);
        indicator.setState(Indicator.State.GREEN);
        return indicator;
    }

    private StatusPanel getParentIndicatorPanel() {
        if (parentIndicatorPanel == null) {
            parentIndicatorPanel = new ParentIndicatorPanel(6, false);

            instancesIndicator = newIndicator(parentIndicatorPanel, "Pipelines",
                "Pipeline instance(s) executing normally", "No pipeline instances executing");
            instancesIndicator.setState(Indicator.State.GRAY);

            workersIndicator = newIndicator(parentIndicatorPanel, "Workers",
                "Worker threads active", "All worker threads idle");
            workersIndicator.setState(Indicator.State.GRAY);

            processesIndicator = newIndicator(parentIndicatorPanel, "Processes",
                "All processes running normally", null);

            alertsIndicator = newIndicator(parentIndicatorPanel, "Alerts",
                "No unacknowledged alerts present", null);

            parentIndicatorPanel.add(instancesIndicator);
            parentIndicatorPanel.add(workersIndicator);
            parentIndicatorPanel.add(processesIndicator);
            parentIndicatorPanel.add(alertsIndicator);
        }
        return parentIndicatorPanel;
    }

    private JPanel getDetailedPanel() {
        if (detailedPanel == null) {
            detailedPanel = new JPanel();
            detailedPanelLayout = new CardLayout();
            detailedPanel.setLayout(detailedPanelLayout);

            detailedPanel.add(getWorkerPanel(), "workers");
            detailedPanel.add(getAlertsPanel(), "alerts");
            detailedPanel.add(getProcessesPanel(), "processes");
            detailedPanel.add(new JPanel(), "metrics");
        }
        return detailedPanel;
    }

    private StatusPanel getProcessesPanel() {
        if (processesPanel == null) {
            processesPanel = ProcessesIndicatorPanel.processesIndicatorPanel(processesIndicator);
        }
        return processesPanel;
    }

    private MonitoringAlertsPanel getAlertsPanel() {
        if (alertsPanel == null) {
            alertsPanel = new MonitoringAlertsPanel();
        }
        return alertsPanel;
    }

    private StatusPanel getWorkerPanel() {
        if (workersPanel == null) {
            workersPanel = new WorkerStatusPanel();
        }
        return workersPanel;
    }

    private WorkerStatusPanel getWorkerStatusPanel() {
        return workersPanel;
    }

    @Override
    public void clicked(Indicator source) {
        if (source == workersIndicator) {
            detailedPanelLayout.show(detailedPanel, "workers");
        } else if (source == instancesIndicator) {
            ZiggyGuiConsole.displayOperationsTab();
        } else if (source == processesIndicator) {
            detailedPanelLayout.show(detailedPanel, "processes");
        } else if (source == alertsIndicator) {
            detailedPanelLayout.show(detailedPanel, "alerts");
        }
    }

    private static void initializeSingletonInstance() {
        if (instance == null) {
            instance = new MasterStatusPanel();
        }
    }

    /**
     * Returns the singleton instance for inclusion in the master {@link ZiggyGuiConsole} GUI.
     */
    public static MasterStatusPanel masterStatusPanel() {
        initializeSingletonInstance();
        return instance;
    }

    public static void setDividerLocation(double d) {
        initializeSingletonInstance();
        instance.getSplitPane().setDividerLocation(d);
    }

    public static Indicator workersIndicator() {
        initializeSingletonInstance();
        return instance.workersIndicator;
    }

    public static Indicator processesIndicator() {
        initializeSingletonInstance();
        return instance.processesIndicator;
    }

    public static Indicator instancesIndicator() {
        initializeSingletonInstance();
        return instance.instancesIndicator;
    }

    public static Indicator alertsIndicator() {
        initializeSingletonInstance();
        return instance.alertsIndicator;
    }

    public static AlertMessageTableModel alertMessageTableModel() {
        initializeSingletonInstance();
        return instance.getAlertsPanel().getAlertMessageTableModel();
    }

    public static WorkerStatusPanel processesStatusPanel() {
        initializeSingletonInstance();
        return instance.getWorkerStatusPanel();
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        MasterStatusPanel masterStatusPanel = new MasterStatusPanel();
        frame.getContentPane().add(masterStatusPanel);
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        frame.setSize(1024, 700);
        masterStatusPanel.splitPane.setDividerLocation(0.35);
    }
}

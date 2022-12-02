package gov.nasa.ziggy.ui.mon.master;

import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;

import gov.nasa.ziggy.services.process.ProcessInfo;

public class ProcessIndicator extends Indicator {
    private static final long serialVersionUID = -5828233116180226266L;

    LabelValue hostLV = new LabelValue("host", "quantum:21434");
    LabelValue stateLV = new LabelValue("state", "Running");
    private JPopupMenu popupMenu;
    private JMenuItem shutdownMenuItem;
    private JMenuItem restartMenuItem;
    private JMenuItem resumeMenuItem;
    private JMenuItem pauseMenuItem;
    LabelValue uptimeLV = new LabelValue("UT", "12d 03h 16m 34s");

    private ProcessInfo processInfo = null;

    public ProcessIndicator(IndicatorPanel parentIndicatorPanel, String name,
        ProcessInfo processInfo) {
        super(parentIndicatorPanel, name);
        this.processInfo = processInfo;
        setIndicatorDisplayName(getHost());
        initGUI();
    }

    public ProcessIndicator(IndicatorPanel parentIndicatorPanel, String name, String host,
        String state, String uptime, ProcessInfo processInfo) {
        super(parentIndicatorPanel, name);
        this.processInfo = processInfo;
        hostLV.setValue(host);
        stateLV.setValue(state);
        uptimeLV.setValue(uptime);
        initGUI();
    }

    private void initGUI() {
        setPreferredSize(new java.awt.Dimension(220, 70));

        addDataComponent(hostLV);
        addDataComponent(stateLV);
        addDataComponent(uptimeLV);
    }

    /**
     * @return Returns the host.
     */
    public String getHost() {
        return hostLV.getValue();
    }

    /**
     * @param host The host to set.
     */
    public void setHost(String host) {
        hostLV.setValue(host);
    }

    /**
     * @return Returns the state.
     */
    public String getLVState() {
        return stateLV.getValue();
    }

    /**
     * @param state The state to set.
     */
    public void setState(String state) {
        stateLV.setValue(state);
    }

    /**
     * @return Returns the uptime.
     */
    public String getUptime() {
        return uptimeLV.getValue();
    }

    /**
     * @param uptime The uptime to set.
     */
    public void setUptime(String uptime) {
        uptimeLV.setValue(uptime);
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }
        });
    }

}

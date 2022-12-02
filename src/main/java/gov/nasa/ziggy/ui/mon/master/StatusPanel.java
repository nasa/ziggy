package gov.nasa.ziggy.ui.mon.master;

import javax.swing.JPanel;

import gov.nasa.ziggy.services.process.StatusMessage;

/**
 * Superclass for panel that receive updates via JMS {@link StatusMessage}s.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public abstract class StatusPanel extends JPanel {
    public abstract void update(StatusMessage statusMessage);
}

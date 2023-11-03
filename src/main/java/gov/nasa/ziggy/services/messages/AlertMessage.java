package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.alert.Alert;

/**
 * Notifies message clients of a new {@link Alert}.
 *
 * @author PT
 */
public class AlertMessage extends PipelineMessage {
    private static final long serialVersionUID = 20230511L;

    private final Alert alertData;

    public AlertMessage(Alert alertData) {
        this.alertData = alertData;
    }

    public Alert getAlertData() {
        return alertData;
    }
}

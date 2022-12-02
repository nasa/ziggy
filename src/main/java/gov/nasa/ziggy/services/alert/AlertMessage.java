package gov.nasa.ziggy.services.alert;

import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.MessageHandler;

public class AlertMessage extends PipelineMessage {
    private static final long serialVersionUID = 20210318L;

    private final Alert alertData;

    public AlertMessage(Alert alertData) {
        this.alertData = alertData;
    }

    public Alert getAlertData() {
        return alertData;
    }

    @Override
    public Object handleMessage(MessageHandler handler) {

        handler.handleAlert(this);
        return null;
    }
}

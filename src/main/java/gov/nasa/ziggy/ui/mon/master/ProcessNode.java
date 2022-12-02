package gov.nasa.ziggy.ui.mon.master;

import gov.nasa.ziggy.services.process.ProcessInfo;
import gov.nasa.ziggy.services.process.StatusMessage;

public class ProcessNode extends StatusNode {

    private ProcessInfo processInfo;

    public ProcessNode(StatusMessage statusMessage) {
        update(statusMessage);
    }

    @Override
    public void update(StatusMessage statusMessage) {
        processInfo = new ProcessInfo(statusMessage.getSourceProcess());
    }

    @Override
    public String toString() {
        return processInfo.toString();
    }

    public String getName() {
        return processInfo.getName();
    }

    public String getHost() {
        return processInfo.getHost();
    }

    public int getPid() {
        return processInfo.getPid();
    }

    public int getJvmid() {
        return processInfo.getJvmid();
    }

}

package gov.nasa.ziggy.module;

import gov.nasa.ziggy.module.SubtaskServer.ResponseType;

public class SubtaskAllocation {
    private SubtaskServer.ResponseType status = SubtaskServer.ResponseType.OK;
    private int subtaskIndex = -1;

    public SubtaskAllocation(ResponseType status, int subtaskIndex) {
        this.status = status;
        this.subtaskIndex = subtaskIndex;
    }

    public SubtaskServer.ResponseType getStatus() {
        return status;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public String toString() {
        return "SubtaskAllocation [response=" + status + ", subtaskIndex=" + subtaskIndex + "]";
    }
}

package gov.nasa.ziggy.metrics.report;

import java.util.Objects;

public class NodeIndexRange {

    private final int startNodeIndex;
    private final int endNodeIndex;

    public NodeIndexRange(int startNodeIndex, int endNodeIndex) {
        this.startNodeIndex = startNodeIndex;
        this.endNodeIndex = endNodeIndex;
    }

    public int getStartNodeIndex() {
        return startNodeIndex;
    }

    public int getEndNodeIndex() {
        return endNodeIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(endNodeIndex, startNodeIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NodeIndexRange other = (NodeIndexRange) obj;
        return endNodeIndex == other.endNodeIndex && startNodeIndex == other.startNodeIndex;
    }
}

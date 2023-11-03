package gov.nasa.ziggy.pipeline.definition;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.JoinTable;

/**
 * This class models a path to a {@link PipelineDefinitionNode} in a {@link PipelineDefinition}. The
 * path is represented with a list of indices (child index), so it does not reference specific
 * instances of nodes, just their location in the tree. use cases
 * <ol>
 * <li>construct when creating trigger def
 * <li>store with trigger def (hibernate pojo)
 * <li>pipeline launch, binding params to instance node in PipelineExecutor.
 * </ol>
 *
 * @author Todd Klaus
 */
@Embeddable
public class PipelineDefinitionNodePath {

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineDefinitionNode_path")
    List<Integer> path = new LinkedList<>();

    protected PipelineDefinitionNodePath() {
    }

    public PipelineDefinitionNodePath(List<Integer> path) {
        if (path == null || path.size() == 0) {
            throw new IllegalStateException("path must be length 1 or greater");
        }
        this.path = path;
    }

    /**
     * Copy constructor
     *
     * @param other
     */
    public PipelineDefinitionNodePath(PipelineDefinitionNodePath other) {
        path.addAll(other.path);
    }

    public PipelineDefinitionNode definitionNodeAt(PipelineDefinition pipelineDefinition) {
        return definitionNodeAt(pipelineDefinition.getRootNodes(), 0);
    }

    private PipelineDefinitionNode definitionNodeAt(List<PipelineDefinitionNode> nodes,
        int pathIndex) {
        int childIndex = path.get(pathIndex);

        if (childIndex >= nodes.size()) {
            return null;
        }
        PipelineDefinitionNode node = nodes.get(childIndex);

        if (pathIndex < path.size() - 1) {
            return definitionNodeAt(node.getNextNodes(), pathIndex + 1);
        }
        // last element of the path
        return node;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Integer i : path) {
            if (!first) {
                sb.append(",");
            }
            sb.append(i);
            first = false;
        }
        return sb.toString();
    }

    public List<Integer> getPath() {
        return path;
    }

    public void setPath(List<Integer> path) {
        this.path = path;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PipelineDefinitionNodePath other = (PipelineDefinitionNodePath) obj;
        if (!Objects.equals(path, other.path)) {
            return false;
        }
        return true;
    }
}

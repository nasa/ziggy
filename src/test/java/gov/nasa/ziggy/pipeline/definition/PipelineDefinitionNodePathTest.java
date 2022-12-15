package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;

public class PipelineDefinitionNodePathTest {
    /**
     * o / \ n1 n2 / \ n3 n4
     *
     * @throws Exception
     */
    @Test
    public void testPath() throws Exception {
        PipelineDefinition pd = new PipelineDefinition();
        PipelineDefinitionNode n1 = new PipelineDefinitionNode();
        PipelineDefinitionNode n2 = new PipelineDefinitionNode();
        PipelineDefinitionNode n3 = new PipelineDefinitionNode();
        PipelineDefinitionNode n4 = new PipelineDefinitionNode();

        pd.getRootNodes().add(n1);
        pd.getRootNodes().add(n2);
        n2.getNextNodes().add(n3);
        n2.getNextNodes().add(n4);

        PipelineDefinitionNodePath n1ExpectedPath = new PipelineDefinitionNodePath(parseList(0));
        PipelineDefinitionNodePath n2ExpectedPath = new PipelineDefinitionNodePath(parseList(1));
        PipelineDefinitionNodePath n3ExpectedPath = new PipelineDefinitionNodePath(parseList(1, 0));
        PipelineDefinitionNodePath n4ExpectedPath = new PipelineDefinitionNodePath(parseList(1, 1));

        pd.buildPaths();

        ReflectionEquals comparer = new ReflectionEquals();

        comparer.assertEquals("n1", n1ExpectedPath, n1.getPath());
        comparer.assertEquals("n2", n2ExpectedPath, n2.getPath());
        comparer.assertEquals("n3", n3ExpectedPath, n3.getPath());
        comparer.assertEquals("n4", n4ExpectedPath, n4.getPath());
    }

    private List<Integer> parseList(int... pathElements) {
        List<Integer> path = new ArrayList<>();

        for (int pathElement : pathElements) {
            path.add(pathElement);
        }

        return path;
    }
}

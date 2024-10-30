package gov.nasa.ziggy.crud;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;

/**
 * Unit tests for {@link AbstractCrud}.
 *
 * @author Bill Wohler
 */
public class AbstractCrudTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AbstractCrudTest.class);

    private TestCrud testCrud;
    private ZiggyQuery<PipelineTask, Long> ziggyQuery;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        testCrud = spy(new TestCrud());
        ziggyQuery = mock(ZiggyQuery.class);
    }

    /** Shows that the list 1, 2, 3, 4, 5 is read in three chunks when maxExpressions == 2. */
    @Test
    public void testChunkedQuery() {
        doReturn(2).when(testCrud).maxExpressions();
        doReturn(List.of(1L, 2L), List.of(3L, 4L), List.of(5L)).when(testCrud).list(ziggyQuery);

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
            testCrud.chunkedPipelineTaskIds(List.of(1L, 2L, 3L, 4L, 5L)));

        verify(testCrud, times(3)).list(ziggyQuery);
    }

    private class TestCrud extends AbstractCrud<PipelineTask> {
        public List<Long> chunkedPipelineTaskIds(List<Long> pipelineTaskIds) {
            doReturn(ziggyQuery).when(ziggyQuery).column(PipelineTask_.id);
            doReturn(ziggyQuery).when(ziggyQuery).select();
            doReturn(ziggyQuery).when(ziggyQuery).in(anyList());

            return chunkedQuery(pipelineTaskIds,
                chunk -> list(ziggyQuery.column(PipelineTask_.id).select().in(chunk)));
        }

        @Override
        public Class<PipelineTask> componentClass() {
            return PipelineTask.class;
        }
    }
}

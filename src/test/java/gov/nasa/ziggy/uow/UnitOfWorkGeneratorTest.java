package gov.nasa.ziggy.uow;

import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;

/**
 * Unit tests for {@link UnitOfWorkGenerator} class.
 *
 * @author PT
 */
public class UnitOfWorkGeneratorTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule pipelineDefaultUowIdentifierClassPropertyRule = new ZiggyPropertyRule(
        PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS,
        "gov.nasa.ziggy.uow.UnitOfWorkGeneratorTest$SampleUnitOfWorkIdentifier");

    /**
     * Tests that units of work are correctly generated; in particular, that the UOW generator and
     * brief state are populated.
     */
    @Test
    public void testGenerateUnitsOfWork() {

        SampleUnitOfWorkGenerator generator = new SampleUnitOfWorkGenerator();
        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(generator, null);
        assertEquals(1, uowList.size());
        UnitOfWork uow = uowList.get(0);
        assertEquals("sample brief state", uow.briefState());
        assertEquals("gov.nasa.ziggy.uow.UnitOfWorkGeneratorTest.SampleUnitOfWorkGenerator",
            uow.getParameter("uowGenerator").getString());
    }

    /**
     * Super-basic UOW generator.
     *
     * @author PT
     */
    private static class SampleUnitOfWorkGenerator implements UnitOfWorkGenerator {

        @Override
        public List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode) {
            UnitOfWork uow = new UnitOfWork();
            List<UnitOfWork> uowList = new ArrayList<>();
            uowList.add(uow);
            return uowList;
        }

        @Override
        public void setBriefState(UnitOfWork uow, PipelineInstanceNode pipelineInstanceNode) {
            uow.setBriefState("sample brief state");
        }
    }
}

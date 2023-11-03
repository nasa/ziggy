package gov.nasa.ziggy.uow;

import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataReceiptPipelineModule;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrudTest;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

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
        List<UnitOfWork> uowList = generator.generateUnitsOfWork(null);
        assertEquals(1, uowList.size());
        UnitOfWork uow = uowList.get(0);
        assertEquals("sample brief state", uow.briefState());
        assertEquals("gov.nasa.ziggy.uow.UnitOfWorkGeneratorTest.SampleUnitOfWorkGenerator",
            uow.getParameter("uowGenerator").getString());
    }

    /**
     * Tests that the Ziggy-side default UOW is correctly identified.
     */
    @Test
    public void testDefaultUnitOfWorkGenerator() {

        Class<?> generator = UnitOfWorkGenerator
            .defaultUnitOfWorkGenerator(ExternalProcessPipelineModule.class);
        assertEquals(DatastoreDirectoryUnitOfWorkGenerator.class, generator);
        generator = UnitOfWorkGenerator.defaultUnitOfWorkGenerator(DataReceiptPipelineModule.class);
        assertEquals(DataReceiptUnitOfWorkGenerator.class, generator);
    }

    /**
     * Tests that the correct exception is thrown when a class lacks a default UOW generator.
     */
    @Test(expected = PipelineException.class)
    public void testNoDefaultGenerator() {
        // Clear property set in property rule.
        ZiggyConfiguration.reset();
        UnitOfWorkGenerator.defaultUnitOfWorkGenerator(PipelineTaskCrudTest.TestModule.class);
    }

    /**
     * Tests that an external UOW generator is correctly handled.
     */
    @Test
    public void testExternalDefaultIdentifier() {
        Class<?> generator = UnitOfWorkGenerator
            .defaultUnitOfWorkGenerator(PipelineTaskCrudTest.TestModule.class);
        assertEquals(SingleUnitOfWorkGenerator.class, generator);
        generator = UnitOfWorkGenerator.defaultUnitOfWorkGenerator(DataReceiptPipelineModule.class);
        assertEquals(DataReceiptUnitOfWorkGenerator.class, generator);
    }

    /**
     * Tests that the UOW generator is correctly retrieved in both the normal and default cases.
     */
    @Test
    public void testUowGeneratorFromNodeDefinition() {

        PipelineDefinitionNode node = new PipelineDefinitionNode();
        node.setUnitOfWorkGenerator(new ClassWrapper<>(SingleUnitOfWorkGenerator.class));
        ClassWrapper<UnitOfWorkGenerator> generator = UnitOfWorkGenerator.unitOfWorkGenerator(node);
        assertEquals("gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator", generator.getClassName());

        // Now test a node with no UOW generator specified and ensure that the default is retrieved.
        node.setUnitOfWorkGenerator(null);
        PipelineModuleDefinition modDef = new PipelineModuleDefinition("the-module");
        modDef.setPipelineModuleClass(new ClassWrapper<>(ExternalProcessPipelineModule.class));
        node.setModuleName(modDef.getName());
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
            crud.persist(modDef);
            return null;
        });
        generator = UnitOfWorkGenerator.unitOfWorkGenerator(node);
        assertEquals("gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator",
            generator.getClassName());
    }

    /**
     * Super-basic UOW generator.
     *
     * @author PT
     */
    private static class SampleUnitOfWorkGenerator implements UnitOfWorkGenerator {

        @Override
        public List<Class<? extends ParametersInterface>> requiredParameterClasses() {
            return new ArrayList<>();
        }

        @Override
        public List<UnitOfWork> generateTasks(
            Map<Class<? extends ParametersInterface>, ParametersInterface> parameters) {
            UnitOfWork uow = new UnitOfWork();
            List<UnitOfWork> uowList = new ArrayList<>();
            uowList.add(uow);
            return uowList;
        }

        @Override
        public String briefState(UnitOfWork uow) {
            return "sample brief state";
        }
    }

    /**
     * Super-basic default UOW generator identifier.
     *
     * @author PT
     */
    @SuppressWarnings("unused")
    private static class SampleUnitOfWorkIdentifier extends DefaultUnitOfWorkIdentifier {

        public SampleUnitOfWorkIdentifier() {
        }

        @Override
        public Class<? extends UnitOfWorkGenerator> defaultUnitOfWorkGeneratorForClass(
            Class<? extends PipelineModule> module) {
            Class<? extends UnitOfWorkGenerator> defaultUowGenerator = null;
            if (module.equals(PipelineTaskCrudTest.TestModule.class)) {
                return SingleUnitOfWorkGenerator.class;
            }
            return defaultUowGenerator;
        }
    }
}

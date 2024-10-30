package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Tests the ability of data receipt to use an externally-defined {@link DataReceiptDefinition}
 * class.
 *
 * @author PT
 */
public class DataReceiptExternalDefinitionTest {

    @Rule
    public ZiggyDatabaseRule ziggyDatabaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule uowProertyRule = new ZiggyPropertyRule(
        PropertyName.DATA_IMPORTER_CLASS.property(),
        "gov.nasa.ziggy.data.management.ExternalDataReceiptDefinition");

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    public ZiggyPropertyRule dataReceiptDirRule = new ZiggyPropertyRule(
        PropertyName.DATA_RECEIPT_DIR.property(), directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(dataReceiptDirRule);

    private PipelineTask pipelineTask;
    private TestDataReceiptPipelineModule module;

    @Before
    public void setUp() {
        pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.getUnitOfWork()).thenReturn(new UnitOfWork());
        module = new TestDataReceiptPipelineModule(pipelineTask, RunMode.STANDARD);
        module = Mockito.spy(module);
        Mockito.doNothing().when(module).incrementProcessingStep();
    }

    @Test
    public void testExternalDataReceiptDefinition() {

        // Start with the things that are done in the executing task action.
        module.executingTaskAction();
        DataReceiptDefinition dataReceiptDefinition = module.dataReceiptDefinition();
        assertEquals(ExternalDataReceiptDefinition.class, dataReceiptDefinition.getClass());
        ExternalDataReceiptDefinition externalDefinition = (ExternalDataReceiptDefinition) dataReceiptDefinition;
        assertTrue(externalDefinition.isPipelineTaskSet());
        assertTrue(externalDefinition.isDataImportDirectorySet());
        assertTrue(externalDefinition.isConformingDeliveryChecked());
        assertTrue(externalDefinition.isConformingFileChecked());
        assertTrue(externalDefinition.isFilesForImportDetermined());
        assertFalse(externalDefinition.isFilesImported());
        assertFalse(externalDefinition.isSuccessfulImportsDetermined());
        assertFalse(externalDefinition.isFailedImportsDetermined());
        assertFalse(externalDefinition.isDataReceiptDirectoryCleaningChecked());

        // Move on to the storing task action.
        module.storingTaskAction();
        assertTrue(externalDefinition.isFilesImported());
        assertTrue(externalDefinition.isSuccessfulImportsDetermined());
        assertTrue(externalDefinition.isFailedImportsDetermined());
        assertTrue(externalDefinition.isDataReceiptDirectoryCleaningChecked());
    }

    private class TestDataReceiptPipelineModule extends DataReceiptPipelineModule {

        public TestDataReceiptPipelineModule(PipelineTask pipelineTask, RunMode runMode) {
            super(pipelineTask, runMode);
        }

        @Override
        protected void persistProducerConsumerRecords(Collection<Path> successfulImports,
            Collection<Path> failedImports) {
        }

        @Override
        Path dataImportPathForTask(UnitOfWork uow) {
            return directoryRule.directory();
        }
    }
}

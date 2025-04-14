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
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
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
    public ZiggyPropertyRule uowPropertyRule = new ZiggyPropertyRule(
        PropertyName.DATA_IMPORTER_CLASS.property(),
        "gov.nasa.ziggy.data.management.ExternalDataReceiptDefinition");

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    public ZiggyPropertyRule dataReceiptDirRule = new ZiggyPropertyRule(
        PropertyName.DATA_RECEIPT_DIR.property(), directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(dataReceiptDirRule);

    private PipelineTask pipelineTask;
    private TestDataReceiptPipelineStepExecutor pipelineStepExecutor;

    @Before
    public void setUp() {
        pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.getUnitOfWork()).thenReturn(new UnitOfWork());
        pipelineStepExecutor = new TestDataReceiptPipelineStepExecutor(pipelineTask,
            RunMode.STANDARD);
        pipelineStepExecutor = Mockito.spy(pipelineStepExecutor);
        Mockito.doNothing().when(pipelineStepExecutor).incrementProcessingStep();
    }

    @Test
    public void testExternalDataReceiptDefinition() {

        // Start with the things that are done in the executing task action.
        pipelineStepExecutor.executingTaskAction();
        DataReceiptDefinition dataReceiptDefinition = pipelineStepExecutor.dataReceiptDefinition();
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
        pipelineStepExecutor.storingTaskAction();
        assertTrue(externalDefinition.isFilesImported());
        assertTrue(externalDefinition.isSuccessfulImportsDetermined());
        assertTrue(externalDefinition.isFailedImportsDetermined());
        assertTrue(externalDefinition.isDataReceiptDirectoryCleaningChecked());
    }

    private class TestDataReceiptPipelineStepExecutor extends DataReceiptPipelineStepExecutor {

        public TestDataReceiptPipelineStepExecutor(PipelineTask pipelineTask, RunMode runMode) {
            super(pipelineTask, runMode);
        }

        @Override
        protected void persistProducerConsumerRecords(Collection<Path> successfulImports,
            Collection<Path> failedImports) {
        }

        @Override
        protected Path dataImportPathForTask(UnitOfWork uow) {
            return directoryRule.directory();
        }
    }
}

package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;

/**
 * Tests that a user-defined unit of work generator for data receipt is correctly managed.
 *
 * @author PT
 */
public class DataReceiptExternalUnitOfWorkTest {

    @Rule
    public ZiggyDatabaseRule ziggyDatabaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule uowProertyRule = new ZiggyPropertyRule(
        PropertyName.DATA_RECEIPT_UOW_GENERATOR_CLASS.property(),
        "gov.nasa.ziggy.data.management.DataReceiptExternalUnitOfWorkTest$TestDataReceiptUowGenerator");

    @Test
    public void testExternalUowDefinition() {
        new PipelineModuleDefinitionOperations().createDataReceiptPipelineModule();
        PipelineModuleDefinition dataReceiptDefinition = new PipelineModuleDefinitionOperations()
            .pipelineModuleDefinition("data-receipt");
        assertNotNull(dataReceiptDefinition);
        assertEquals(TestDataReceiptUowGenerator.class,
            dataReceiptDefinition.getUnitOfWorkGenerator().getClazz());
    }

    private class TestDataReceiptUowGenerator extends DataReceiptUnitOfWorkGenerator {

    }
}

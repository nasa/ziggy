package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;

import gov.nasa.ziggy.data.management.DataReceiptPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Operations class for methods that mainly deal with {@link PipelineModuleDefinition} instances.
 *
 * @author PT
 */
public class PipelineModuleDefinitionOperations extends DatabaseOperations {

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();

    public List<PipelineModuleDefinition> allPipelineModuleDefinitions() {
        return performTransaction(() -> pipelineModuleDefinitionCrud().retrieveAll());
    }

    public List<PipelineModuleDefinition> pipelineModuleDefinitions() {
        return performTransaction(() -> pipelineModuleDefinitionCrud().retrieveLatestVersions());
    }

    public PipelineModuleDefinition pipelineModuleDefinition(String moduleName) {
        return performTransaction(
            () -> pipelineModuleDefinitionCrud().retrieveLatestVersionForName(moduleName));
    }

    public PipelineModuleDefinition merge(PipelineModuleDefinition module) {
        return performTransaction(() -> pipelineModuleDefinitionCrud().merge(module));
    }

    public PipelineModuleDefinition rename(PipelineModuleDefinition module, String newName) {
        return performTransaction(() -> pipelineModuleDefinitionCrud().rename(module, newName));
    }

    public void delete(PipelineModuleDefinition module) {
        performTransaction(() -> pipelineModuleDefinitionCrud().remove(module));
    }

    public PipelineModuleExecutionResources pipelineModuleExecutionResources(
        PipelineModuleDefinition module) {
        return performTransaction(
            () -> pipelineModuleDefinitionCrud().retrieveExecutionResources(module));
    }

    public PipelineModuleExecutionResources merge(
        PipelineModuleExecutionResources executionResources) {
        return performTransaction(() -> pipelineModuleDefinitionCrud().merge(executionResources));
    }

    /** Creates and persists the data receipt pipeline module definition. */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public PipelineModuleDefinition createDataReceiptPipelineModule() {
        // Create the data receipt pipeline module
        PipelineModuleDefinition dataReceiptModule = new PipelineModuleDefinition(
            DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME);
        ClassWrapper<PipelineModule> moduleClassWrapper = new ClassWrapper<>(
            DataReceiptPipelineModule.class);
        dataReceiptModule.setPipelineModuleClass(moduleClassWrapper);
        String uowGeneratorClassname = ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATA_RECEIPT_UOW_GENERATOR_CLASS.property(),
                DataReceiptPipelineModule.DEFAULT_DATA_RECEIPT_UOW_GENERATOR_CLASS);
        try {
            Class<?> uowGeneratorClass = Class.forName(uowGeneratorClassname);
            if (!UnitOfWorkGenerator.class.isAssignableFrom(uowGeneratorClass)) {
                throw new PipelineException("Class " + uowGeneratorClassname
                    + " is not an instance of UnitOfWorkGenerator");
            }
            dataReceiptModule.setUnitOfWorkGenerator(
                new ClassWrapper<>((Class<UnitOfWorkGenerator>) uowGeneratorClass));
        } catch (ClassNotFoundException e) {
            throw new PipelineException("Unable to locate class " + uowGeneratorClassname);
        }
        return performTransaction(() -> pipelineModuleDefinitionCrud().merge(dataReceiptModule));
    }

    public ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(String moduleName) {
        return performTransaction(
            () -> pipelineModuleDefinitionCrud().retrieveUnitOfWorkGenerator(moduleName));
    }

    public void lock(PipelineModuleDefinition pipelineModuleDefinition) {
        performTransaction(() -> {
            PipelineModuleDefinition module = pipelineModuleDefinitionCrud()
                .retrieveLatestVersionForName(pipelineModuleDefinition.getName());
            module.lock();
            pipelineModuleDefinitionCrud().merge(module);
        });
    }

    PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud() {
        return pipelineModuleDefinitionCrud;
    }
}

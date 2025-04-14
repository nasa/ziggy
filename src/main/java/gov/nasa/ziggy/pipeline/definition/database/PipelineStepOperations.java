package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;

import gov.nasa.ziggy.data.management.DataReceiptPipelineStepExecutor;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.PipelineStepExecutionResources;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Operations class for methods that mainly deal with {@link PipelineStep} instances.
 *
 * @author PT
 */
public class PipelineStepOperations extends DatabaseOperations {

    private PipelineStepCrud pipelineStepCrud = new PipelineStepCrud();

    public List<PipelineStep> allPipelineSteps() {
        return performTransaction(() -> pipelineStepCrud().retrieveAll());
    }

    public List<PipelineStep> pipelineSteps() {
        return performTransaction(() -> pipelineStepCrud().retrieveLatestVersions());
    }

    public PipelineStep pipelineStep(String pipelineStepName) {
        return performTransaction(
            () -> pipelineStepCrud().retrieveLatestVersionForName(pipelineStepName));
    }

    public PipelineStep merge(PipelineStep pipelineStep) {
        return performTransaction(() -> pipelineStepCrud().merge(pipelineStep));
    }

    public PipelineStepExecutionResources pipelineStepExecutionResources(
        PipelineStep pipelineStep) {
        return performTransaction(
            () -> pipelineStepCrud().retrieveExecutionResources(pipelineStep));
    }

    public PipelineStepExecutionResources merge(PipelineStepExecutionResources executionResources) {
        return performTransaction(() -> pipelineStepCrud().merge(executionResources));
    }

    /** Creates and persists the data receipt pipeline step. */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public PipelineStep createDataReceiptPipelineStep() {
        // Create the data receipt pipeline step.
        PipelineStep dataReceiptStep = new PipelineStep(
            DataReceiptPipelineStepExecutor.DATA_RECEIPT_PIPELINE_STEP_EXECUTOR_NAME);
        ClassWrapper<PipelineStepExecutor> pipelineStepExecutorClassWrapper = new ClassWrapper<>(
            DataReceiptPipelineStepExecutor.class);
        dataReceiptStep.setPipelineStepExecutorClass(pipelineStepExecutorClassWrapper);
        String uowGeneratorClassname = ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATA_RECEIPT_UOW_GENERATOR_CLASS.property(),
                DataReceiptPipelineStepExecutor.DEFAULT_DATA_RECEIPT_UOW_GENERATOR_CLASS);
        try {
            Class<?> uowGeneratorClass = Class.forName(uowGeneratorClassname);
            if (!UnitOfWorkGenerator.class.isAssignableFrom(uowGeneratorClass)) {
                throw new PipelineException("Class " + uowGeneratorClassname
                    + " is not an instance of UnitOfWorkGenerator");
            }
            dataReceiptStep.setUnitOfWorkGenerator(
                new ClassWrapper<>((Class<UnitOfWorkGenerator>) uowGeneratorClass));
        } catch (ClassNotFoundException e) {
            throw new PipelineException("Unable to locate class " + uowGeneratorClassname);
        }
        return performTransaction(() -> pipelineStepCrud().merge(dataReceiptStep));
    }

    public ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(String pipelineStepName) {
        return performTransaction(
            () -> pipelineStepCrud().retrieveUnitOfWorkGenerator(pipelineStepName));
    }

    public void lock(PipelineStep pipelineStep) {
        performTransaction(() -> {
            PipelineStep currentPipelineStep = pipelineStepCrud()
                .retrieveLatestVersionForName(pipelineStep.getName());
            currentPipelineStep.lock();
            pipelineStepCrud().merge(currentPipelineStep);
        });
    }

    PipelineStepCrud pipelineStepCrud() {
        return pipelineStepCrud;
    }
}

package gov.nasa.ziggy.pipeline.definition.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.AlgorithmPipelineStepExecutor;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Performs conditioning of imported pipeline step definitions to a state that can be persisted to
 * the database.
 * <p>
 * Specifically, conditioning includes the following:
 * <ol>
 * <li>Rejection of any update imports if the update flag is not set.
 * <li>Performing any update imports if the update flag is set.
 * <li>Checking for invalid or missing UOW generators.
 * </ol>
 *
 * @author PT
 */
public class PipelineStepImportConditioner {

    private static final Logger log = LoggerFactory.getLogger(PipelineStepImportConditioner.class);

    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();

    public void conditionPipelineSteps(List<PipelineStep> pipelineSteps, boolean update) {
        List<PipelineStep> stepsWithoutUpdate = new ArrayList<>();
        List<String> databasePipelineStepNames = pipelineStepOperations().allPipelineSteps()
            .stream()
            .map(PipelineStep::getName)
            .collect(Collectors.toList());
        for (PipelineStep pipelineStep : pipelineSteps) {
            if (databasePipelineStepNames.contains(pipelineStep.getName())) {
                if (!update) {
                    log.warn("Pipeline step {} already present in database, not importing",
                        pipelineStep.getName());
                    System.out.println("Pipeline step " + pipelineStep.getName()
                        + " already present in database, not importing");
                    stepsWithoutUpdate.add(pipelineStep);
                    continue;
                }
                log.info("Updating definition of pipeline step {}", pipelineStep.getName());
            } else {
                log.info("Creating new pipeline step {}", pipelineStep.getName());
            }

            // Additional validation:
            // PipelineStepExecutor must not have a UOW generator in its XML,
            // except for one that subclasses DatastoreDirectoryUnitOfWorkGenerator.
            // All other pipeline step classes must have a UOW generator in their XMLs.
            if (pipelineStep.getPipelineStepExecutorClass()
                .getClazz()
                .equals(AlgorithmPipelineStepExecutor.class)) {
                if (pipelineStep.getUnitOfWorkGenerator() == null) {
                    pipelineStep.setUnitOfWorkGenerator(
                        new ClassWrapper<>(DatastoreDirectoryUnitOfWorkGenerator.class));
                }
                if (!DatastoreDirectoryUnitOfWorkGenerator.class
                    .isAssignableFrom(pipelineStep.getUnitOfWorkGenerator().getClazz())) {
                    throw new PipelineException("Pipeline step " + pipelineStep.getName()
                        + " uses PipelineStepExecutor, specified UOW "
                        + pipelineStep.getUnitOfWorkGenerator().getClazz().toString()
                        + " is invalid");
                }
            } else if (pipelineStep.getUnitOfWorkGenerator() == null) {
                throw new PipelineException("Pipeline step " + pipelineStep.getName()
                    + " must specify a unit of work generator");
            }
        }

        // Remove any steps that should not be persisted.
        pipelineSteps.removeAll(stepsWithoutUpdate);
    }

    PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }
}

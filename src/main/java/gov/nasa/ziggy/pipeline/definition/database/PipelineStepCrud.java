package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.PipelineStep_;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Provides CRUD methods for {@link PipelineStep}
 *
 * @author Todd Klaus
 */
public class PipelineStepCrud extends UniqueNameVersionPipelineComponentCrud<PipelineStep> {

    public PipelineStepCrud() {
    }

    public List<PipelineStep> retrieveAll() {
        return list(
            createZiggyQuery(PipelineStep.class).column(PipelineStep_.NAME).ascendingOrder());
    }

    public ClassWrapper<UnitOfWorkGenerator> retrieveUnitOfWorkGenerator(String pipelineStepName) {
        return retrieveLatestVersionForName(pipelineStepName).getUnitOfWorkGenerator();
    }

    @Override
    public String componentNameForExceptionMessages() {
        return "pipeline step";
    }

    @Override
    public Class<PipelineStep> componentClass() {
        return PipelineStep.class;
    }
}

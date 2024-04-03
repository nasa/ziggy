package gov.nasa.ziggy.pipeline.definition;

import gov.nasa.ziggy.crud.SimpleCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Creates a debug pipeline task object. This is useful for testing hibernate objects that need a
 * pipeline task for referential integrity.
 *
 * @author Sean McCauliff
 */
public class FakePipelineTaskFactory {
    public PipelineTask newTask() {
        return newTask(true);
    }

    public PipelineTask newTask(boolean inDb) {

        PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();

        PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
        PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();

        return (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {

            // create a module param set def
            ParameterSet parameterSet = new ParameterSet("test mps1");
            parameterSet.setTypedParameters(new TestModuleParameters().getParameters());
            if (inDb) {
                parameterSet = parameterSetCrud.merge(parameterSet);
            }

            // create a module def
            PipelineModuleDefinition moduleDef = new PipelineModuleDefinition("Test-1");
            PipelineDefinition pipelineDef = new PipelineDefinition("test pipeline name");
            if (inDb) {
                moduleDef = pipelineModuleDefinitionCrud.merge(moduleDef);
                pipelineDef = pipelineDefinitionCrud.merge(pipelineDef);
            }

            // create some pipeline def nodes
            PipelineDefinitionNode pipelineDefNode1 = new PipelineDefinitionNode(
                moduleDef.getName(), pipelineDef.getName());
            moduleDef.setUnitOfWorkGenerator(new ClassWrapper<>(new SingleUnitOfWorkGenerator()));
            pipelineDefNode1 = new SimpleCrud<>().merge(pipelineDefNode1);
            pipelineDef.getRootNodes().add(pipelineDefNode1);
            if (inDb) {
                pipelineDef = pipelineDefinitionCrud.merge(pipelineDef);
            }

            PipelineInstance pipelineInstance = new PipelineInstance(pipelineDef);
            pipelineInstance.putParameterSet(new ClassWrapper<>(new TestPipelineParameters()),
                parameterSet);
            if (inDb) {
                pipelineInstanceCrud.persist(pipelineInstance);
            }

            PipelineInstanceNode pipelineInstanceNode1 = new PipelineInstanceNode(pipelineInstance,
                pipelineDefNode1, moduleDef);
            if (inDb) {
                pipelineInstanceNodeCrud.persist(pipelineInstanceNode1);
            }

            PipelineTask task = new PipelineTask(pipelineInstance, pipelineInstanceNode1);
            task.setUowTaskParameters(new UnitOfWork().getParameters());
            task.setWorkerHost("test worker name");
            task.setSoftwareRevision("42");
            if (inDb) {
                pipelineTaskCrud.persist(task);
            }
            return task;
        });
    }

    public static void main(String[] argv) throws Exception {
        PipelineTask task = (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {
            FakePipelineTaskFactory me = new FakePipelineTaskFactory();
            return me.newTask();
        });
        System.out.println("Created task with id " + task.getId());
    }
}

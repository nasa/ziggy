package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

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
        UserCrud userCrud = new UserCrud();

        PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();

        PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
        PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();

        return (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {

            // create users
            User testUser = new User("unit-test", "Unit-Test", "unit-test@example.com", "x111");
            if (inDb) {
                userCrud.createUser(testUser);
            }

            // create a module param set def
            ParameterSet parameterSet = new ParameterSet(new AuditInfo(testUser, new Date()),
                "test mps1");
            parameterSet.setParameters(new BeanWrapper<Parameters>(new TestModuleParameters()));
            if (inDb) {
                parameterSetCrud.create(parameterSet);
            }

            // create a module def
            PipelineModuleDefinition moduleDef = new PipelineModuleDefinition("Test-1");
            PipelineDefinition pipelineDef = new PipelineDefinition(
                new AuditInfo(testUser, new Date()), "test pipeline name");
            if (inDb) {
                pipelineModuleDefinitionCrud.create(moduleDef);
                pipelineDefinitionCrud.create(pipelineDef);

            }

            // create some pipeline def nodes
            PipelineDefinitionNode pipelineDefNode1 = new PipelineDefinitionNode(
                moduleDef.getName(), pipelineDef.getName().getName());
            pipelineDefNode1.setUnitOfWorkGenerator(
                new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
            pipelineDefNode1.setStartNewUow(true);

            pipelineDef.getRootNodes().add(pipelineDefNode1);
            if (inDb) {
                pipelineDefinitionCrud.create(pipelineDef);
            }

            PipelineInstance pipelineInstance = new PipelineInstance(pipelineDef);
            pipelineInstance.putParameterSet(
                new ClassWrapper<Parameters>(new TestPipelineParameters()), parameterSet);
            if (inDb) {
                pipelineInstanceCrud.create(pipelineInstance);
            }

            PipelineInstanceNode pipelineInstanceNode1 = new PipelineInstanceNode(pipelineInstance,
                pipelineDefNode1, moduleDef);
            if (inDb) {
                pipelineInstanceNodeCrud.create(pipelineInstanceNode1);
            }

            PipelineTask task = new PipelineTask(pipelineInstance, pipelineInstanceNode1);
            task.setUowTask(new BeanWrapper<>(new UnitOfWork()));
            task.setWorkerHost("test worker name");
            task.setSoftwareRevision("42");
            if (inDb) {
                pipelineTaskCrud.create(task);
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

package gov.nasa.ziggy.uow;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Subclass of {@link DirectoryUnitOfWorkGenerator} that selects units of work based on the
 * directory tree configuration of the datastore. This is the default unit of work for the
 * {@link ExternalProcessPipelineModule} class.
 *
 * @author PT
 */
public class DatastoreDirectoryUnitOfWorkGenerator extends DirectoryUnitOfWorkGenerator {

    private boolean singleSubtask;
    public static final String SINGLE_SUBTASK_PROPERTY_NAME = "singleSubtask";

    /**
     * Convenience method that returns the value of the single subtask property.
     */
    public static boolean singleSubtask(UnitOfWork uow) {
        String clazz = uow.getParameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME)
            .getString();
        try {
            Class<?> cls = Class.forName(clazz);
            if (DatastoreDirectoryUnitOfWorkGenerator.class.isAssignableFrom(cls)) {
                return (Boolean) uow.getParameter(SINGLE_SUBTASK_PROPERTY_NAME).getValue();
            } else {
                throw new PipelineException(
                    "Class " + clazz + " not a subclass of DatastoreDirectoryUnitOfWorkGenerator");
            }
        } catch (ClassNotFoundException e) {
            throw new PipelineException("Generator class " + clazz + " not found", e);
        }

    }

    @Override
    protected Path rootDirectory() {
        return DirectoryProperties.datastoreRootDir();
    }

    /**
     * Extends the {@link DirectoryUnitOfWorkGenerator#generateTasks(Map)} method. Specifically, the
     * superclass method is used for the initial generation of the UOW instances, following which
     * the value of the singleSubtask field is set according to the value of
     * {@link TaskConfigurationParameters#isSingleSubtask()}.
     */
    @Override
    public List<UnitOfWork> generateTasks(Map<Class<? extends Parameters>, Parameters> parameters) {
        List<UnitOfWork> tasks = super.generateTasks(parameters);
        TaskConfigurationParameters taskConfigurationParameters = (TaskConfigurationParameters) parameters
            .get(TaskConfigurationParameters.class);
        boolean singleSubtask = taskConfigurationParameters.isSingleSubtask();
        for (UnitOfWork uow : tasks) {
            uow.addParameter(new TypedParameter(SINGLE_SUBTASK_PROPERTY_NAME,
                Boolean.toString(singleSubtask), ZiggyDataType.ZIGGY_BOOLEAN));
        }
        return tasks;
    }

    /**
     * Determines whether processing of the data in this UOW is performed in a single subtask or in
     * multiple subtasks. Multiple subtasks are appropriate for the situation in which the
     * processing is "embarrassingly parallel" (i.e., there are numerous chunks of data and there
     * are no dependencies between the chunks, thus each chunk can be processed independently of any
     * others). Single subtask processing is appropriate for situations in which all the data must
     * be processed together due to dependencies between the data (a simple example: averaging all
     * the chunks of data together requires them to be processed together, hence single subtask
     * would be used in such a case).
     */
    public boolean isSingleSubtask() {
        return singleSubtask;
    }

    public void setSingleSubtask(boolean singleSubtask) {
        this.singleSubtask = singleSubtask;
    }

}

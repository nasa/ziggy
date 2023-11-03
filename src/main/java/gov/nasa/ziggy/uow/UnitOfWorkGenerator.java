package gov.nasa.ziggy.uow;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.management.DataReceiptPipelineModule;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Interface for all unit of work generators. A unit of work generator constructs instances of the
 * {@link UnitOfWork} class that can be used by pipeline modules to determine which set of data the
 * corresponding pipeline task should process (for example: a time range).
 * <p>
 * Implementations of {@link UnitOfWorkGenerator} are required to provide the following methods:
 * <ol>
 * <li>{@link #requiredParameterClasses()}, which specifies the implementations of
 * {@link Parameters} that the generator requires to generate units of work. During UOW generation,
 * {@link UnitOfWorkGenerator} will ensure that instances of each required parameter class have been
 * supplied as arguments to {@link #generateTasks(Map)}.
 * <li>{@link #generateTasks(Map)}, which returns a {@link List} of {@link UnitOfWork} instances.
 * The method can make use of the {@link Parameters} instances passed to it in the form of a
 * {@link Map}.
 * <li>{@link #briefState(UnitOfWork), which generates a brief state {@link String} for an instance
 * based on its properties, and adds same to the properties collection in the {@link UnitOfWork}
 * instance. This is used for display purposes, and so should be informative about what the UOW
 * represents.
 *
 * @author Todd Klaus
 * @author PT
 */
public interface UnitOfWorkGenerator {

    String GENERATOR_CLASS_PARAMETER_NAME = "uowGenerator";

    static Map<Class<? extends PipelineModule>, Class<? extends UnitOfWorkGenerator>> ziggyDefaultUowGenerators() {
        return ImmutableMap.of(ExternalProcessPipelineModule.class,
            DatastoreDirectoryUnitOfWorkGenerator.class, DataReceiptPipelineModule.class,
            DataReceiptUnitOfWorkGenerator.class);
    }

    /**
     * Should return the {@link Parameters} classes required by this task generator, or and empty
     * list if no {@link Parameters} classes are required.
     * <p>
     * Used by the console to prevent misconfigurations.
     */
    List<Class<? extends ParametersInterface>> requiredParameterClasses();

    /**
     * Generate the task objects for this unit of work. This method must be supplied by the
     * implementing class. It is used in conjunction with {@link #briefState(UnitOfWork)} to
     * generate all UOWs for all tasks.
     */
    List<UnitOfWork> generateTasks(
        Map<Class<? extends ParametersInterface>, ParametersInterface> parameters);

    /**
     * Generates the content of the briefState property based on the other properties in a UOW. It
     * is used in conjunction with {@link #generateTasks(Map)} to generate all UOWs for all tasks.
     */
    String briefState(UnitOfWork uow);

    /**
     * Generates the set of UOWs using the {@link #generateTasks(Map)} and
     * {@link #briefState(UnitOfWork)} methods of a given implementation. The resulting
     * {@link UnitOfWork} instance will also contain a property that specifies the class name of the
     * generator.
     */
    default List<UnitOfWork> generateUnitsOfWork(
        Map<Class<? extends ParametersInterface>, ParametersInterface> parameters) {

        // Produce the tasks and sort by brief state
        List<UnitOfWork> uows = generateTasks(parameters);

        // Add some metadata parameters to all the instances.
        for (UnitOfWork uow : uows) {
            uow.addParameter(new TypedParameter(UnitOfWork.BRIEF_STATE_PARAMETER_NAME,
                briefState(uow), ZiggyDataType.ZIGGY_STRING));
            uow.addParameter(new TypedParameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
                getClass().getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        }

        // Now that the UOWs have their brief states properly assigned, sort them by brief state
        // and return.
        return uows.stream().sorted().collect(Collectors.toList());
    }

    /**
     * Obtains the UOW generator for a pipeline node. The generator is the one in the database for
     * that node, unless that unit of work is {@link DefaultUnitOfWork}; in that case, the default
     * unit of work must be looked up and returned.
     */
    static ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineDefinitionNode node) {

        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = node.getUnitOfWorkGenerator();
        if (unitOfWorkGenerator == null) {

            // Get the current definition of the pipeline module
            PipelineModuleDefinition module = new PipelineModuleDefinitionCrud()
                .retrieveLatestVersionForName(node.getModuleName());
            Class<? extends PipelineModule> moduleClass = module.getPipelineModuleClass()
                .getClazz();
            Class<? extends UnitOfWorkGenerator> uowClass = defaultUnitOfWorkGenerator(moduleClass);
            unitOfWorkGenerator = new ClassWrapper<>(uowClass);
        }
        return unitOfWorkGenerator;
    }

    /**
     * Determines the {@link UnitOfWorkGenerator} class that serves as the default UOW for a given
     * subclass of {@link PipelineModule}. The user-specified implementation of
     * {@link DefaultUnitOfWorkIdentifier}, if any, is used first. If the UOW class is not found in
     * that implementation, or if that implementation does not exist, the
     * {@link #ziggyDefaultUowGenerators()} is tried as a fallback.
     */
    static Class<? extends UnitOfWorkGenerator> defaultUnitOfWorkGenerator(
        Class<? extends PipelineModule> module) {

        Class<? extends UnitOfWorkGenerator> defaultUnitOfWork = null;

        // Start by using the pipeline-side manager for default UOWs, if any is specified
        String pipelineUowManagerClassName = ZiggyConfiguration.getInstance()
            .getString(PropertyName.PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS.property(), null);
        if (pipelineUowManagerClassName != null) {

            // Try to instantiate the Pipeline-side default UOW generator, and throw an exception
            // if unable to do so.
            try {
                DefaultUnitOfWorkIdentifier pipelineManager = (DefaultUnitOfWorkIdentifier) Class
                    .forName(pipelineUowManagerClassName)
                    .getConstructor()
                    .newInstance();
                defaultUnitOfWork = pipelineManager.defaultUnitOfWorkGeneratorForClass(module);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
                | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                | SecurityException e) {
                throw new PipelineException("Pipeline default unit of work generator class "
                    + pipelineUowManagerClassName + " could not be instantiated", e);
            }
        }

        if (defaultUnitOfWork == null) { // try the Ziggy version
            defaultUnitOfWork = ziggyDefaultUowGenerators().get(module);
        }

        // If we still don't have a default UOW generator, throw an exception, since the only case
        // in which this method is called is when the user specified that the default generator for
        // a module was supposed to be used, and if the module doesn't have one then we have to fail
        // out.
        if (defaultUnitOfWork == null) {
            throw new PipelineException(
                "Unable to locate default UOW generator for " + module.getName());
        }
        return defaultUnitOfWork;
    }
}

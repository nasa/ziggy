package gov.nasa.ziggy.pipeline;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * This is a convenience class for creating pipeline and trigger definitions. To keep the API
 * simple, branching nodes are not supported.
 *
 * @author Todd Klaus
 */
public class PipelineConfigurator {
    private static final Logger log = LoggerFactory.getLogger(PipelineConfigurator.class);

    private final PipelineDefinitionCrud pipelineDefinitionCrud;
    private final PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private final ParameterSetCrud parameterSetCrud;

    PipelineDefinition pipeline = null;
    PipelineDefinitionNode currentNode = null;

    // pipeline params
    private Map<ClassWrapper<ParametersInterface>, String> pipelineParameterSetNamesMap = new HashMap<>();

    // module params
    private Map<PipelineDefinitionNode, Map<ClassWrapper<ParametersInterface>, String>> moduleParameterSetNamesMap = new HashMap<>();

    private int exeTimeout = 60 * 60 * 50; // 50 hours

    public PipelineConfigurator() {

        pipelineDefinitionCrud = new PipelineDefinitionCrud();
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
    }

    /**
     * Create a new pipeline definition. Must be called before nodes are added.
     *
     * @param name
     */
    public PipelineDefinition createPipeline(String name) {
        // delete old pipeline definition, if it exists
        pipelineDefinitionCrud.deleteAllVersionsForName(name);

        pipeline = new PipelineDefinition(name);
        pipeline.setDescription("Created by PipelineConfigurator");

        return pipeline;
    }

    /**
     * Convenience method to create a new pipeline definition with a single pipeline
     * {@link Parameters} class. More pipeline {@link Parameters} classes can be added with
     * addPipelineParametersClass()
     *
     * @param name
     * @param pipelineParams
     * @throws PipelineException
     */
    public PipelineDefinition createPipeline(String name, Parameters pipelineParams) {
        createPipeline(name);

        ParameterSet pipelineParamSet = createParamSet(name + "-params", pipelineParams);
        pipelineParamSet.setDescription("default pipeline params created by PipelineConfigurator");

        pipelineParameterSetNamesMap.put(new ClassWrapper<>(pipelineParams),
            pipelineParamSet.getName());

        return pipeline;
    }

    /**
     * Add a new {@link Parameters} class this this {@link PipelineDefinition}s list of pipeline
     * parameters classes
     */
    public void addPipelineParameters(String name, Parameters pipelineParams) {
        if (pipeline == null) {
            throw new PipelineException("Pipeline not initialized, call createPipeline() first");
        }

        ParameterSet pipelineParamSet = createParamSet(name, pipelineParams);
        pipelineParameterSetNamesMap.put(new ClassWrapper<>(pipelineParams),
            pipelineParamSet.getName());
    }

    /**
     * Add a param set to the param set names to be used for the next trigger
     *
     * @param parameterSet
     */
    public void addPipelineParameterSet(ParameterSet parameterSet) {
        if (pipeline == null) {
            throw new PipelineException("Pipeline not initialized, call createPipeline() first");
        }

        Class<? extends ParametersInterface> paramSetClass = parameterSet.parametersInstance()
            .getClass();

        ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(paramSetClass);
        pipelineParameterSetNamesMap.put(classWrapper, parameterSet.getName());
    }

    /**
     * Convenience method to create a new {@link PipelineModuleDefinition} and a new
     * {@link PipelineDefinitionNode} to hold it in one step. This method should only be used if the
     * {@link PipelineModuleDefinition} won't be shared by multiple pipelines. If shared, use
     * createModule() to create it and call addNode(PipelineModuleDefinition moduleDef)
     *
     * @param name
     * @param clazz
     * @param exeName
     * @param parametersList
     * @param taskGenerator
     * @return
     * @throws PipelineException
     */
    public PipelineDefinitionNode addNode(String name, Class<? extends PipelineModule> clazz,
        String exeName, UnitOfWorkGenerator taskGenerator, Parameters... parametersList) {
        if (pipeline == null) {
            throw new PipelineException("Pipeline not initialized, call createPipeline() first");
        }

        List<ParameterSet> paramSets = new LinkedList<>();
        if (parametersList != null) {
            for (Parameters parameters : parametersList) {
                ParameterSet paramSet = createParamSet(
                    name + "-" + parameters.getClass().getSimpleName(), parameters);
                paramSet.setDescription("default module params created by PipelineConfigurator");
                paramSets.add(paramSet);
            }
        }

        return createNode(name, clazz, exeName, taskGenerator,
            paramSets.toArray(new ParameterSet[0]));
    }

    /**
     * @param name
     * @param clazz
     * @param exeName
     * @param taskGenerator
     * @param paramSets
     * @return
     */
    private PipelineDefinitionNode createNode(String name, Class<? extends PipelineModule> clazz,
        String exeName, UnitOfWorkGenerator taskGenerator, ParameterSet... paramSets) {
        PipelineModuleDefinition moduleDef = createModule(name, clazz, exeName);

        PipelineDefinitionNode node = addNode(moduleDef, taskGenerator);

        Map<ClassWrapper<ParametersInterface>, String> paramSetNamesMap = new HashMap<>();

        for (ParameterSet set : paramSets) {
            ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(
                set.parametersInstance().getClass());
            paramSetNamesMap.put(classWrapper, set.getName());
        }

        moduleParameterSetNamesMap.put(node, paramSetNamesMap);

        return node;
    }

    /**
     * @param name
     * @param clazz
     * @param exeName
     * @param taskGenerator
     * @param paramSets
     * @return
     */
    public PipelineDefinitionNode addNode(String name, Class<? extends PipelineModule> clazz,
        String exeName, UnitOfWorkGenerator taskGenerator, ParameterSet... paramSets) {
        return createNode(name, clazz, exeName, taskGenerator, paramSets);
    }

    /**
     * Add a node to the pipeline with the specified {@link PipelineModuleDefinition} and no
     * {@link UnitOfWorkGenerator}. This means that a simple transition will be used using the unit
     * of work from the previous node. Cannot be used for the first node in a pipeline.
     * <p>
     * TODO: verify that the previous node has the same UOW?
     *
     * @param moduleDef
     * @return
     * @throws PipelineException
     */
    public PipelineDefinitionNode addNode(PipelineModuleDefinition moduleDef) {
        return addNode(moduleDef, null, new ParameterSet[0]);
    }

    /**
     * Add a node to the pipeline with the specified {@link PipelineModuleDefinition} and
     * {@link UnitOfWorkGenerator}
     *
     * @param moduleDef
     * @return
     * @throws PipelineException
     */
    public PipelineDefinitionNode addNode(PipelineModuleDefinition moduleDef,
        ParameterSet... parameterSets) {
        return addNode(moduleDef, null, parameterSets);
    }

    /**
     * Add a node to the pipeline with the specified {@link PipelineModuleDefinition} and
     * {@link UnitOfWorkGenerator}
     *
     * @param moduleDef
     * @param taskGenerator
     * @return
     * @throws PipelineException
     * @deprecated Use varargs version instead
     */
    @Deprecated
    public PipelineDefinitionNode addNode(PipelineModuleDefinition moduleDef,
        UnitOfWorkGenerator taskGenerator, List<ParameterSet> paramSets) {
        return addNode(moduleDef, taskGenerator, paramSets.toArray(new ParameterSet[0]));
    }

    /**
     * An alternate version of the above method that uses var args
     *
     * @param moduleDef
     * @param taskGenerator
     * @param paramSets
     * @return
     */
    public PipelineDefinitionNode addNode(PipelineModuleDefinition moduleDef,
        UnitOfWorkGenerator taskGenerator, ParameterSet... paramSets) {
        if (pipeline == null) {
            throw new IllegalStateException(
                "Pipeline not initialized, call createPipeline() first");
        }

        PipelineDefinitionNode node = new PipelineDefinitionNode();
        node.setPipelineModuleDefinition(moduleDef);

        Map<ClassWrapper<ParametersInterface>, String> paramSetNamesMap = new HashMap<>();

        for (ParameterSet set : paramSets) {
            Class<? extends ParametersInterface> paramClass = set.parametersInstance().getClass();

            ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(paramClass);
            paramSetNamesMap.put(classWrapper, set.getName());
        }

        moduleParameterSetNamesMap.put(node, paramSetNamesMap);

        if (taskGenerator != null) {
            node.setUnitOfWorkGenerator(new ClassWrapper<>(taskGenerator));
            node.setStartNewUow(true);
        } else {
            node.setStartNewUow(false);
        }

        if (currentNode != null) {
            currentNode.getNextNodes().add(node);
        } else {
            // first node
            if (taskGenerator == null) {
                throw new IllegalStateException(
                    "UnitOfWorkTaskGenerator for the first node in a pipeline must not be null");
            }
            pipeline.getRootNodes().add(node);
        }

        currentNode = node;

        return node;
    }

    /**
     * Create a shared {@link PipelineModuleDefinition}
     *
     * @param name
     * @param clazz
     * @return
     */
    public PipelineModuleDefinition createModule(String name,
        Class<? extends PipelineModule> clazz) {
        return createModule(name, clazz, null);
    }

    /**
     * Create a shared {@link PipelineModuleDefinition} with the specified exeName and
     * {@link Parameters} class names
     *
     * @param name
     * @param clazz
     * @param exeName
     * @return
     */
    public PipelineModuleDefinition createModule(String name, Class<? extends PipelineModule> clazz,
        String exeName) {
        // delete any existing pipeline modules with this name
        List<PipelineModuleDefinition> existingModules = pipelineModuleDefinitionCrud
            .retrieveAllVersionsForName(name);
        for (PipelineModuleDefinition existingModule : existingModules) {
            log.info("deleting existing pipeline module def: " + existingModule);
            pipelineModuleDefinitionCrud.remove(existingModule);
        }

        PipelineModuleDefinition moduleDef = new PipelineModuleDefinition(name);
        moduleDef.setPipelineModuleClass(new ClassWrapper<>(clazz));
        moduleDef.setExeTimeoutSecs(exeTimeout);

        return pipelineModuleDefinitionCrud.merge(moduleDef);
    }

    /**
     * Create a shared {@link ParameterSet}
     *
     * @param name
     * @param params
     * @return
     * @throws PipelineException
     */
    public ParameterSet createParamSet(String name, Parameters params) {
        // delete any existing PipelineModuleParameterSets
        List<ParameterSet> existingParamSets = parameterSetCrud.retrieveAllVersionsForName(name);
        for (ParameterSet set : existingParamSets) {
            log.info("deleting existing pipeline module param set: " + set);
            parameterSetCrud.remove(set);
        }

        ParameterSet paramSet = new ParameterSet(name);
        paramSet.setDescription("Created by PipelineConfigurator");

        paramSet.populateFromParametersInstance(params);

        return parameterSetCrud.merge(paramSet);
    }

    /**
     * Set the param set names to be used for the next trigger
     *
     * @param pipelineParams
     */
    public void setPipelineParamNames(
        Map<ClassWrapper<ParametersInterface>, String> pipelineParams) {
        pipelineParameterSetNamesMap = pipelineParams;
    }

    public void setModuleParamNames(PipelineDefinitionNode node,
        Map<ClassWrapper<ParametersInterface>, String> moduleParams) {
        moduleParameterSetNamesMap.put(node, moduleParams);
    }

    public void addModuleParamNames(PipelineDefinitionNode node, ParameterSet parameterSet) {
        Map<ClassWrapper<ParametersInterface>, String> moduleParamsForNode = moduleParameterSetNamesMap
            .get(node);
        ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(
            parameterSet.parametersInstance().getClass());
        moduleParamsForNode.put(classWrapper, parameterSet.getName());
    }

    /**
     * Persist the current pipeline definition and reset for the next pipeline definition.
     *
     * @throws PipelineException
     */
    public void finalizePipeline() {
        if (pipeline == null) {
            throw new IllegalStateException(
                "Pipeline not initialized, call createPipeline() first");
        }

        if (currentNode == null) {
            throw new IllegalStateException("Pipeline has no nodes, call addNode() at least once");
        }

        pipelineDefinitionCrud.persist(pipeline);

        pipeline = null;
        currentNode = null;
    }

    /**
     * @return the exeTimeout
     */
    public int getExeTimeout() {
        return exeTimeout;
    }

    /**
     * @param exeTimeout the exeTimeout to set
     */
    public void setExeTimeout(int exeTimeout) {
        this.exeTimeout = exeTimeout;
    }
}

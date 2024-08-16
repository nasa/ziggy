package gov.nasa.ziggy.pipeline.definition.database;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.data.datastore.DatastoreConfigurationImporter;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionImporter;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.pipeline.xml.ParameterImportExportOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.services.database.DatabaseController;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.services.events.ZiggyEventHandlerDefinitionImporter;

/**
 * Provides operations methods for the import of pipeline definitions.
 *
 * @author PT
 */
public class PipelineImportOperations extends DatabaseOperations {

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
    private PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();
    private PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();
    private ParameterImportExportOperations parameterImportExportOperations = new ParameterImportExportOperations();

    /**
     * Persist the collection of pipelines, modules, and nodes imported from XML. This must all be
     * done in a single transaction so that an error causes the entire import to roll back.
     */
    public void persistDefinitions(
        Map<PipelineModuleDefinition, PipelineModuleExecutionResources> resourcesByPipelineModule,
        Map<PipelineDefinition, Set<PipelineDefinitionNodeExecutionResources>> resourcesByNode) {
        performTransaction(() -> {
            for (Map.Entry<PipelineModuleDefinition, PipelineModuleExecutionResources> entry : resourcesByPipelineModule
                .entrySet()) {
                pipelineModuleDefinitionCrud().merge(entry.getValue());
                pipelineModuleDefinitionCrud().merge(entry.getKey());
            }
            for (Map.Entry<PipelineDefinition, Set<PipelineDefinitionNodeExecutionResources>> entry : resourcesByNode
                .entrySet()) {
                pipelineDefinitionCrud().merge(entry.getKey());
                for (PipelineDefinitionNodeExecutionResources resources : entry.getValue()) {
                    pipelineDefinitionNodeCrud().merge(resources);
                }
            }
        });
    }

    /**
     * Performs the import of files that constitute a cluster definition. These are the data file
     * types, parameter sets, pipeline and module definitions, and event handler definitions. All of
     * the imports must be persisted in a single transaction so that, in the event of an exception,
     * the entire import gets rolled back.
     */
    public void importClusterDefinitions(File[] parameterFiles, List<String> dataTypeFileNames,
        List<File> pipelineDefinitionFiles, File[] eventHandlerFiles,
        DatabaseController databaseController) {
        performTransaction(new NonReturningDatabaseTransaction() {

            @Override
            public void transaction() {
                for (File parameterFile : parameterFiles) {
                    parameterImportExportOperations().importParameterLibrary(parameterFile, null,
                        ParamIoMode.STANDARD);
                }
                new DatastoreConfigurationImporter(dataTypeFileNames, false).importConfiguration();
                new PipelineDefinitionImporter()
                    .importPipelineConfiguration(pipelineDefinitionFiles);
                new ZiggyEventHandlerDefinitionImporter(eventHandlerFiles).importFromFiles();
            }

            @Override
            public void finallyBlock() {
                if (databaseController != null) {
                    databaseController.stop();
                }
            }
        });
    }

    PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud() {
        return pipelineModuleDefinitionCrud;
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        return pipelineDefinitionCrud;
    }

    PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud() {
        return pipelineDefinitionNodeCrud;
    }

    ParameterImportExportOperations parameterImportExportOperations() {
        return parameterImportExportOperations;
    }
}

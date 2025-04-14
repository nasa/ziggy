package gov.nasa.ziggy.pipeline.definition.database;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.HibernateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineProcessingOptions;
import gov.nasa.ziggy.pipeline.definition.PipelineProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineProcessingOptions_;
import gov.nasa.ziggy.pipeline.definition.Pipeline_;

/**
 * Provides CRUD methods for {@link Pipeline}
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineCrud extends UniqueNameVersionPipelineComponentCrud<Pipeline> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineCrud.class);

    public PipelineCrud() {
    }

    public List<Pipeline> retrieveAll() {
        return list(createZiggyQuery(Pipeline.class));
    }

    @Override
    public Pipeline retrieve(String name, int version) {
        return super.retrieve(name, version);
    }

    /**
     * Retrieves the names of all {@link Pipeline}s that are associated with
     * {@link PipelineInstance}s.
     *
     * @return a non-{@code null} list of {@link Pipeline} names.
     * @throws HibernateException if there were problems accessing the database.
     */
    public List<String> retrievePipelineNamesInUse() {
        ZiggyQuery<Pipeline, String> query = createZiggyQuery(Pipeline.class, String.class);
        query.column(Pipeline_.NAME).select();
        query.column(Pipeline_.LOCKED).in(true);
        query.distinct(true);
        return list(query);
    }

    public boolean processingModeExistsInDatabase(String pipelineName) {
        return uniqueResult(createZiggyQuery(PipelineProcessingOptions.class)
            .column(PipelineProcessingOptions_.pipelineName)
            .in(pipelineName)) != null;
    }

    public ProcessingMode retrieveProcessingMode(String pipelineName) {
        return uniqueResult(createZiggyQuery(PipelineProcessingOptions.class, ProcessingMode.class)
            .column(PipelineProcessingOptions_.pipelineName)
            .in(pipelineName)
            .column(PipelineProcessingOptions_.processingMode)
            .select());
    }

    public PipelineProcessingOptions updateProcessingMode(String pipelineName,
        ProcessingMode processingMode) {
        PipelineProcessingOptions pipelineProcessingOptions = uniqueResult(
            createZiggyQuery(PipelineProcessingOptions.class)
                .column(PipelineProcessingOptions_.pipelineName)
                .in(pipelineName));
        pipelineProcessingOptions.setProcessingMode(processingMode);
        return super.merge(pipelineProcessingOptions);
    }

    public Pipeline merge(Pipeline pipeline) {
        if (!processingModeExistsInDatabase(pipeline.getName())) {
            persist(new PipelineProcessingOptions(pipeline.getName()));
        }
        return super.merge(pipeline);
    }

    public List<PipelineNode> retrieveRootNodes(Pipeline pipeline) {
        ZiggyQuery<Pipeline, PipelineNode> query = createZiggyQuery(Pipeline.class,
            PipelineNode.class);
        query.column(Pipeline_.id).in(pipeline.getId());
        query.column(Pipeline_.rootNodes).select();
        return list(query);
    }

    public Set<String> retrieveParameterSetNames(Pipeline pipeline) {
        ZiggyQuery<Pipeline, String> query = createZiggyQuery(Pipeline.class, String.class);
        query.column(Pipeline_.id).in(pipeline.getId());
        query.column(Pipeline_.parameterSetNames).select();
        return new HashSet<>(list(query));
    }

    @Override
    public String componentNameForExceptionMessages() {
        return "pipeline";
    }

    @Override
    public Class<Pipeline> componentClass() {
        return Pipeline.class;
    }
}

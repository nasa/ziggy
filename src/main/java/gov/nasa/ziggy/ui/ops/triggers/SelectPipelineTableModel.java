package gov.nasa.ziggy.ui.ops.triggers;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class SelectPipelineTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(SelectPipelineTableModel.class);

    private List<PipelineDefinition> pipelines = new LinkedList<>();
    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    public SelectPipelineTableModel() {
        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        log.debug("loadFromDatabase() - start");

        if (pipelines != null) {
            pipelineDefinitionCrud.evictAll(pipelines);
        }

        try {
            pipelines = pipelineDefinitionCrud.retrieveLatestVersions();
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

    public PipelineDefinition getPipelineAtRow(int rowIndex) {
        validityCheck();
        return pipelines.get(rowIndex);
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return pipelines.size();
    }

    @Override
    public int getColumnCount() {
        return 2;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        PipelineDefinition pipeline = pipelines.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return pipeline.getId();
            case 1:
                return pipeline.getName();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return String.class;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "ID";
            case 1:
                return "Name";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}

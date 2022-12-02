package gov.nasa.ziggy.ui.config.pipeline;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

@SuppressWarnings("serial")
public class PipelinesTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(PipelinesTableModel.class);

    private List<PipelineDefinition> pipelines = new LinkedList<>();
    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    public PipelinesTableModel() {
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
        return 6;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        PipelineDefinition pipeline = pipelines.get(rowIndex);

        AuditInfo auditInfo = pipeline.getAuditInfo();

        User lastChangedUser = null;
        Date lastChangedTime = null;

        if (auditInfo != null) {
            lastChangedUser = auditInfo.getLastChangedUser();
            lastChangedTime = auditInfo.getLastChangedTime();
        }

        switch (columnIndex) {
            case 0:
                return pipeline.getId();
            case 1:
                return pipeline.getName();
            case 2:
                return pipeline.getVersion();
            case 3:
                return pipeline.isLocked();
            case 4:
                if (lastChangedUser != null) {
                    return lastChangedUser.getLoginName();
                } else {
                    return "---";
                }
            case 5:
                if (lastChangedTime != null) {
                    return lastChangedTime;
                } else {
                    return "---";
                }
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return Long.class;
            case 1:
                return String.class;
            case 2:
                return Integer.class;
            case 3:
                return Boolean.class;
            case 4:
                return String.class;
            case 5:
                return Object.class;
            default:
                return String.class;
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "ID";
            case 1:
                return "Name";
            case 2:
                return "Version";
            case 3:
                return "Locked";
            case 4:
                return "User";
            case 5:
                return "Mod. Time";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }

    @Override
    public boolean isCellEditable(int row, int col) {
        return false;
    }
}

package gov.nasa.ziggy.ui.ops.triggers;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

@SuppressWarnings("serial")
public class TriggersTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(TriggersTableModel.class);

    private List<PipelineDefinition> pipelines = new LinkedList<>();
    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    public TriggersTableModel() {
        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();
    }

    @Override
    public void loadFromDatabase() throws PipelineException {
        try {
            if (pipelines != null) {
                log.info("Clearing the Hibernate cache of all loaded triggers");
                pipelineDefinitionCrud.evictAll(pipelines); // clear the cache
            }

            pipelines = pipelineDefinitionCrud.retrieveLatestVersions();
        } catch (ConsoleSecurityException ignore) {
        }
        fireTableDataChanged();
    }

    /**
     * Returns true if a trigger already exists with the specified name. checked when the operator
     * changes the trigger name so we can warn them before we get a database constraint violation.
     *
     * @param name
     * @return
     */
    public PipelineDefinition triggerByName(String name) {
        validityCheck();
        for (PipelineDefinition triggerDef : pipelines) {
            if (name.equals(triggerDef.getName().getName())) {
                return triggerDef;
            }
        }
        return null;
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return pipelines.size();
    }

    @Override
    public int getColumnCount() {
        return 3;
    }

    public PipelineDefinition getTriggerAt(int rowIndex) {
        validityCheck();
        return pipelines.get(rowIndex);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();
        PipelineDefinition trigger = pipelines.get(rowIndex);

        AuditInfo auditInfo = trigger.getAuditInfo();

        User lastChangedUser = null;
        Date lastChangedTime = null;

        if (auditInfo != null) {
            lastChangedUser = auditInfo.getLastChangedUser();
            lastChangedTime = auditInfo.getLastChangedTime();
        }

        switch (columnIndex) {
            case 0:
                return trigger.getName();
            case 1:
                if (lastChangedUser != null) {
                    return lastChangedUser.getLoginName();
                } else {
                    return "---";
                }
            case 2:
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
        validityCheck();

        switch (columnIndex) {
            case 0:
                return String.class;
            case 1:
                return String.class;
            case 2:
                return Object.class;
            default:
                return String.class;
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Name";
            case 1:
                return "User";
            case 2:
                return "Mod. Time";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}

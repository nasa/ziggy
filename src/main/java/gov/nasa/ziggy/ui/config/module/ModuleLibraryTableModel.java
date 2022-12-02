package gov.nasa.ziggy.ui.config.module;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.PipelineModuleDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ModuleLibraryTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(ModuleLibraryTableModel.class);

    private List<PipelineModuleDefinition> modules = new LinkedList<>();
    private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud;

    public ModuleLibraryTableModel() {
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        log.info("loadFromDatabase() - start");

        if (modules != null) {
            pipelineModuleDefinitionCrud.evictAll(modules);
        }

        try {
            modules = pipelineModuleDefinitionCrud.retrieveLatestVersions();
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

    public PipelineModuleDefinition getModuleAtRow(int rowIndex) {
        validityCheck();
        return modules.get(rowIndex);
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return modules.size();
    }

    @Override
    public int getColumnCount() {
        validityCheck();
        return 6;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        PipelineModuleDefinition module = modules.get(rowIndex);

        AuditInfo auditInfo = module.getAuditInfo();

        User lastChangedUser = null;
        Date lastChangedTime = null;

        if (auditInfo != null) {
            lastChangedUser = auditInfo.getLastChangedUser();
            lastChangedTime = auditInfo.getLastChangedTime();
        }

        switch (columnIndex) {
            case 0:
                return module.getId();
            case 1:
                return module.getName().getName();
            case 2:
                return module.getVersion();
            case 3:
                return module.isLocked();
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
        validityCheck();

        switch (columnIndex) {
            case 0:
                return Integer.class;
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
        validityCheck();
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
}

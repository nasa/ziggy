package gov.nasa.ziggy.ui.config.parameters;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.ParameterSetCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetsTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetsTableModel.class);

    private final ParameterSetCrudProxy parameterSetCrud;
    protected List<ParameterSet> paramSets = new ArrayList<>();

    public ParameterSetsTableModel() {
        parameterSetCrud = new ParameterSetCrudProxy();
    }

    public ParameterSetsTableModel(List<ParameterSet> paramSets) {
        this.paramSets = paramSets;
        parameterSetCrud = new ParameterSetCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        log.debug("loadFromDatabase() - start");

        if (paramSets != null) {
            parameterSetCrud.evictAll(paramSets);
        }

        try {
            paramSets = parameterSetCrud.retrieveLatestVersions();
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

    public void loadByNames(List<ParameterSetName> parameterSetNames) {
        if (paramSets != null) {
            parameterSetCrud.evictAll(paramSets);
            paramSets.clear();
        }

        for (ParameterSetName parameterSetName : parameterSetNames) {
            ParameterSet paramSet = parameterSetCrud.retrieveLatestVersionForName(parameterSetName);
            paramSets.add(paramSet);
        }
    }

    public ParameterSet getParamSetAtRow(int rowIndex) {
        validityCheck();
        return paramSets.get(rowIndex);
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return paramSets.size();
    }

    @Override
    public int getColumnCount() {
        return 6;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        ParameterSet paramSet = paramSets.get(rowIndex);

        AuditInfo auditInfo = paramSet.getAuditInfo();

        User lastChangedUser = null;
        Date lastChangedTime = null;

        if (auditInfo != null) {
            lastChangedUser = auditInfo.getLastChangedUser();
            lastChangedTime = auditInfo.getLastChangedTime();
        }

        String type = "<deleted>";

        try {
            type = paramSet.getParameters().getClazz().getSimpleName();
        } catch (PipelineException e) {
            // ignore if the class is not on the classpath
        }

        switch (columnIndex) {
            case 0:
                return paramSet.getName().getName();
            case 1:
                return type;
            case 2:
                return paramSet.getVersion();
            case 3:
                return paramSet.isLocked();
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
                return String.class;
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
                return "Name";
            case 1:
                return "Type";
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

    public void setParamSets(List<ParameterSet> paramSets) {
        this.paramSets = paramSets;
        fireTableDataChanged();
    }

    public boolean add(ParameterSet e) {
        boolean added = paramSets.add(e);
        fireTableDataChanged();
        return added;
    }

    public boolean remove(ParameterSet paramSet) {
        boolean removed = paramSets.remove(paramSet);
        fireTableDataChanged();
        return removed;
    }

    public ParameterSet remove(int index) {
        ParameterSet removed = paramSets.remove(index);
        fireTableDataChanged();
        return removed;
    }

    public List<ParameterSet> getParamSets() {
        return paramSets;
    }
}

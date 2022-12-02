package gov.nasa.ziggy.ui.ops.parameters;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.ui.common.HtmlLabelBuilder;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetNamesTableModel extends AbstractTableModel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParameterSetNamesTableModel.class);

    private final LinkedList<ParameterSetAssignment> paramSetAssignments = new LinkedList<>();

    public ParameterSetNamesTableModel(
        Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters,
        Set<ClassWrapper<Parameters>> requiredParameters,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters) {
        update(currentParameters, requiredParameters, currentPipelineParameters);
    }

    /**
     * for each required param create a ParameterSetAssignment if reqd param exists in current
     * params, use that name if reqd param exists in current pipeline params, use that name with
     * '(pipeline)' if there are any left in current params (not reqd), add those
     *
     * @param currentParameters
     * @param requiredParameters
     * @param currentPipelineParameters
     */
    public void update(Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters,
        Set<ClassWrapper<Parameters>> requiredParameters,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters) {
        paramSetAssignments.clear();
        Set<ClassWrapper<Parameters>> types = new HashSet<>();

        // for each required param type, create a ParameterSetAssignment
        for (ClassWrapper<Parameters> requiredType : requiredParameters) {
            ParameterSetAssignment param = new ParameterSetAssignment(requiredType);

            // if required param type exists in current params, use that
            // ParameterSetName
            ParameterSetName currentAssignment = currentParameters.get(requiredType);
            if (currentAssignment != null) {
                param.setAssignedName(currentAssignment);
            }

            // if required param type exists in current *pipeline* params,
            // display that (read-only)
            if (currentPipelineParameters.containsKey(requiredType)) {
                param.setAssignedName(currentPipelineParameters.get(requiredType));
                param.setAssignedAtPipelineLevel(true);

                if (currentAssignment != null) {
                    param.setAssignedAtBothLevels(true);
                }
            }

            if (param.isAssignedAtPipelineLevel() || param.isAssignedAtBothLevels()) {
                paramSetAssignments.addFirst(param);
            } else {
                paramSetAssignments.add(param);
            }

            types.add(requiredType);
        }

        // If there are any param types left over in current params (not
        // required), add those
        // This also covers the case where empty lists are passed in for
        // required params and
        // current pipeline params (when using this model to edit pipeline
        // params on the EditTriggerDialog
        for (ClassWrapper<Parameters> currentParam : currentParameters.keySet()) {
            if (!types.contains(currentParam)) {
                ParameterSetAssignment param = new ParameterSetAssignment(currentParam,
                    currentParameters.get(currentParam), false, false);
                paramSetAssignments.add(param);
            }
        }

        fireTableDataChanged();
    }

    public ParameterSetName getParamSetAtRow(int rowIndex) {
        return paramSetAssignments.get(rowIndex).getAssignedName();
    }

    public ParameterSetAssignment getParamAssignmentAtRow(int rowIndex) {
        return paramSetAssignments.get(rowIndex);
    }

    /**
     * @return the paramSetAssignments
     */
    public LinkedList<ParameterSetAssignment> getParamSetAssignments() {
        return paramSetAssignments;
    }

    @Override
    public int getRowCount() {
        return paramSetAssignments.size();
    }

    @Override
    public int getColumnCount() {
        return 2;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        ParameterSetAssignment assignment = paramSetAssignments.get(rowIndex);
        ClassWrapper<Parameters> assignmentType = assignment.getType();
        ParameterSetName assignedName = assignment.getAssignedName();

        HtmlLabelBuilder displayName = new HtmlLabelBuilder();

        if (assignedName != null) {
            displayName.append(assignedName.getName());
        } else {
            displayName.appendColor("--- not set ---", "red");
        }

        if (assignment.isAssignedAtBothLevels()) {
            displayName.appendColor(" (ERROR: set at BOTH levels)", "red");
        } else if (assignment.isAssignedAtPipelineLevel()) {
            displayName.appendItalic(" (set at pipeline level)");
        }

        switch (columnIndex) {
            case 0:
                Class<?> clazz = null;
                try {
                    clazz = assignmentType.getClazz();
                } catch (RuntimeException e) {
                    return "<deleted>: " + assignmentType.getClassName();
                }
                return clazz.getSimpleName();
            case 1:
                return displayName;
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Type";
            case 1:
                return "Name";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}

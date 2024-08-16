package gov.nasa.ziggy.util.dispmod;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * {@link DisplayModel} for pipeline instances. This class is used to format pipeline instances for
 * display on the console.
 *
 * @author Todd Klaus
 */
public class InstancesDisplayModel extends DisplayModel {
    private static final String[] COLUMN_NAMES = { "ID", "Pipeline", "Status", "Time" };

    private List<PipelineInstance> instances = new LinkedList<>();

    public InstancesDisplayModel() {
    }

    public InstancesDisplayModel(List<PipelineInstance> instances) {
        this.instances = instances;
    }

    public InstancesDisplayModel(PipelineInstance instance) {
        instances = new LinkedList<>();
        instances.add(instance);
    }

    public void update(List<PipelineInstance> instances) {
        this.instances = instances;
    }

    @Override
    public int getRowCount() {
        return instances.size();
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    public PipelineInstance getInstanceAt(int rowIndex) {
        return instances.get(rowIndex);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineInstance instance = instances.get(rowIndex);

        return switch (columnIndex) {
            case 0 -> instance.getId();
            case 1 -> instance.getPipelineDefinition().getName()
                + (StringUtils.isBlank(instance.getName()) ? "" : ": " + instance.getName());
            case 2 -> getStateString(instance.getState());
            case 3 -> instance.elapsedTime();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    private String getStateString(PipelineInstance.State state) {
        return state.toString();
    }
}

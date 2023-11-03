package gov.nasa.ziggy.util.dispmod;

import java.util.LinkedList;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * {@link DisplayModel} for pipeline instances. This class is used to format pipeline instances for
 * display on the console.
 *
 * @author Todd Klaus
 */
public class InstancesDisplayModel extends DisplayModel {
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
        return 4;
    }

    public PipelineInstance getInstanceAt(int rowIndex) {
        return instances.get(rowIndex);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineInstance instance = instances.get(rowIndex);

        return switch (columnIndex) {
            case 0 -> instance.getId();
            case 1 -> instance.getPipelineDefinition().getName() + ": " + instance.getName();
            case 2 -> getStateString(instance.getState());
            case 3 -> instance.elapsedTime();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    @Override
    public String getColumnName(int column) {
        return switch (column) {
            case 0 -> "ID";
            case 1 -> "Pipeline Name";
            case 2 -> "State";
            case 3 -> "P-time";
            default -> throw new IllegalArgumentException("Unexpected value: " + column);
        };
    }

    private String getStateString(PipelineInstance.State state) {
        return state.toString();
    }
}

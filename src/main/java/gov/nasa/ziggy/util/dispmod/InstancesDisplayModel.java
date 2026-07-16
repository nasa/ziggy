package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.Date;
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
    private static final String[] COLUMN_NAMES = { "ID", "Pipeline", "Date", "Status", "Time" };
    private static final Class<?>[] COLUMN_CLASSES = { String.class, String.class, Date.class,
        String.class, Integer.class };

    private List<PipelineInstance> instances = new ArrayList<>();

    public InstancesDisplayModel() {
    }

    public InstancesDisplayModel(List<PipelineInstance> instances) {
        this.instances = instances;
    }

    public InstancesDisplayModel(PipelineInstance instance) {
        instances = new ArrayList<>();
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

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return COLUMN_CLASSES[columnIndex];
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineInstance instance = instances.get(rowIndex);

        return switch (columnIndex) {
            case 0 -> instance.getId();
            case 1 -> instance.getPipeline().getName()
                + (StringUtils.isBlank(instance.getName()) ? "" : ": " + instance.getName());
            case 2 -> instance.getCreated();
            case 3 -> getStateString(instance.getState());
            case 4 -> instance.getExecutionClock();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    public PipelineInstance getInstanceAt(int rowIndex) {
        return instances.get(rowIndex);
    }

    private String getStateString(PipelineInstance.State state) {
        return state.toString();
    }
}

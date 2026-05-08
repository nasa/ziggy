package gov.nasa.ziggy.util.dispmod;

import static com.lowagie.text.Element.ALIGN_CENTER;
import static com.lowagie.text.Element.ALIGN_LEFT;
import static com.lowagie.text.Element.ALIGN_RIGHT;

import java.util.ArrayList;
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
    private static final int[] COLUMN_ALIGNMENT = { ALIGN_RIGHT, ALIGN_LEFT, ALIGN_CENTER,
        ALIGN_CENTER, ALIGN_CENTER };

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

    public PipelineInstance getInstanceAt(int rowIndex) {
        return instances.get(rowIndex);
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

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    @Override
    public int getAlignment(int column) {
        return COLUMN_ALIGNMENT[column];
    }

    private String getStateString(PipelineInstance.State state) {
        return state.toString();
    }
}

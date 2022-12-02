package gov.nasa.ziggy.ui.metrilyzer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;

import org.apache.commons.collections4.list.TreeList;

import gov.nasa.ziggy.metrics.MetricType;

/**
 * Wraps the metrics types in a ComboBoxModel.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
abstract class MetricTypeListModel extends DefaultComboBoxModel<String> {
    private TreeList<MetricType> types = new TreeList<>();

    public MetricTypeListModel() {
    }

    protected void updateTypes(Set<MetricType> metricTypes) {
        MetricType[] sortableArray = new MetricType[metricTypes.size()];
        metricTypes.toArray(sortableArray);
        Arrays.sort(sortableArray);
        types = new TreeList<>(Arrays.asList(sortableArray));
        fireContentsChanged(this, 0, sortableArray.length);
    }

    /**
     * Completely refresh the metric types.
     */
    public abstract void loadMetricTypes();

    @Override
    public String getElementAt(int index) {
        return types.get(index).getName();
    }

    @Override
    public int getSize() {
        return types.size();
    }

    @SuppressWarnings("unchecked")
    public void add(MetricType metricType) {
        int insertIndex = Collections.binarySearch(types, metricType);
        if (insertIndex >= 0) {
            // dup
            return;
        }
        insertIndex = -insertIndex - 1;
        types.add(insertIndex, metricType);
        fireContentsChanged(this, insertIndex, insertIndex);
    }

    public MetricType remove(int index) {
        MetricType mt = types.remove(index);
        fireContentsChanged(this, index, index);
        return mt;
    }

    @SuppressWarnings("unchecked")
    public List<MetricType> getTypes() {
        return types;
    }
}

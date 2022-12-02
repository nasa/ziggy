package gov.nasa.ziggy.ui.ops.instances;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel;

@SuppressWarnings("serial")
public class PipelineStatsTableModel extends AbstractTableModel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TaskMetricsTableModel.class);

    private PipelineStatsDisplayModel pipelineStatsDisplayModel = null;

    public PipelineStatsTableModel(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        update(tasks, orderedModuleNames);
    }

    public void update(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        try {
            pipelineStatsDisplayModel = new PipelineStatsDisplayModel(tasks, orderedModuleNames);
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();
    }

    @Override
    public int getRowCount() {
        return pipelineStatsDisplayModel.getRowCount();
    }

    @Override
    public int getColumnCount() {
        return pipelineStatsDisplayModel.getColumnCount();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return pipelineStatsDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    @Override
    public String getColumnName(int column) {
        return pipelineStatsDisplayModel.getColumnName(column);
    }
}

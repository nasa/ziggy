package gov.nasa.ziggy.ui.ops.instances;

import java.util.LinkedList;
import java.util.List;

import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceFilter;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.PipelineInstanceCrudProxy;
import gov.nasa.ziggy.util.dispmod.InstancesDisplayModel;

@SuppressWarnings("serial")
public class InstancesTableModel extends AbstractDatabaseModel {

    private static final Logger log = LoggerFactory.getLogger(InstancesTableModel.class);

    private final InstancesDisplayModel instancesDisplayModel = new InstancesDisplayModel();
    private List<PipelineInstance> instances = new LinkedList<>();
    private final PipelineInstanceCrudProxy pipelineInstanceCrud;
    private final PipelineInstanceFilter filter;

    public InstancesTableModel(PipelineInstanceFilter filter) {
        this.filter = filter;
        pipelineInstanceCrud = new PipelineInstanceCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        SwingWorker<Void, Void> swingWorker = new SwingWorker<>() {

            @Override
            protected Void doInBackground() throws Exception {
                refreshInstancesFromDatabase();
                return null;
            }

            @Override
            protected void done() {
                if (isModelValid()) {
                    refreshGui();
                }

            }

        };
        swingWorker.execute();
    }

    /**
     * Performs the non-GUI portion of refreshing the pipeline instance list from the database. Both
     * the evictAll() and retrieve() calls use the CrudProxyExecutor, which in turn uses a single
     * threaded executor and synchronous execution.
     */
    public void refreshInstancesFromDatabase() {
        if (instances != null) {
            pipelineInstanceCrud.evictAll(instances);
        }
        try {
            instances = pipelineInstanceCrud.retrieve(filter, true);
        } catch (ConsoleSecurityException ignore) {
        }
    }

    /**
     * Performs the GUI portion of refreshing the pipeline instance list from the database. This
     * method must be called from the event dispatch thread for the console. Hence it must be called
     * either directly within that thread or via the invokeLater() or invokeAndWait() methods of
     * SwingUtilities.
     */
    public void refreshGui() {
        try {
            instancesDisplayModel.update(instances);
        } catch (ConsoleSecurityException ignore) {
        }
        fireTableDataChanged();
        log.debug("end of refreshGui()");
    }

    /**
     * Returns the model ID corresponding to a selected pipeline instance ID. If the selected
     * pipeline instance ID is less than zero, or if it is not present in the model, -1 is returned.
     */
    public int getModelIndexOfInstance(long instanceId) {
        int modelId = -1;
        if (instanceId < 0) {
            return modelId;
        }
        for (int i = 0; i < instances.size(); i++) {
            if (instances.get(i).getId() == instanceId) {
                modelId = i;
                break;
            }
        }
        return modelId;
    }

    /**
     * Returns the state of the instance with max ID. Assumes that the list of instances is sorted
     * ascending, which is the expected behavior of the PipelineInstanceCrudProxy retriever.
     *
     * @return State of instance with max ID number.
     */
    public PipelineInstance.State getStateOfInstanceWithMaxid() {
        if (instances.isEmpty()) {
            return PipelineInstance.State.COMPLETED;
        }
        return instances.get(instances.size() - 1).getState();
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return instancesDisplayModel.getRowCount();
    }

    @Override
    public int getColumnCount() {
        validityCheck();
        return instancesDisplayModel.getColumnCount();
    }

    public PipelineInstance getInstanceAt(int rowIndex) {
        validityCheck();
        return instancesDisplayModel.getInstanceAt(rowIndex);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();
        return instancesDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    @Override
    public String getColumnName(int column) {
        validityCheck();
        return instancesDisplayModel.getColumnName(column);
    }
}

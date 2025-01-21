package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceFilter;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;

/**
 * The left hand side subpanel of the instances panel. This side of the panel displays a table of
 * pipeline instances and provides controls for a filter that selects instances based on when they
 * ran, condition, etc.
 *
 * @author PT
 * @author Bill Wohler
 */
public class InstancesPanel extends JPanel {

    private static final long serialVersionUID = 20230817L;

    private InstancesTable instancesTable;

    public InstancesPanel(InstancesTasksPanel instancesTasksPanel) {
        buildComponent(instancesTasksPanel, new PipelineInstanceFilter());
    }

    private void buildComponent(InstancesTasksPanel instancesTasksPanel,
        PipelineInstanceFilter instancesFilter) {
        JLabel instances = boldLabel("Pipeline instances", LabelType.HEADING1);

        InstancesControlPanel instancesControlPanel = new InstancesControlPanel(instancesFilter);
        instancesControlPanel.setListener(this);

        instancesTable = new InstancesTable(instancesTasksPanel, instancesFilter);
        JScrollPane instancesTableScrollPane = new JScrollPane(instancesTable.getTable());

        GroupLayout layout = new GroupLayout(this);
        setLayout(layout);

        layout.setHorizontalGroup(layout.createParallelGroup()
            .addComponent(instances)
            .addComponent(instancesControlPanel)
            .addComponent(instancesTableScrollPane));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(instances)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(instancesControlPanel)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(instancesTableScrollPane, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE,
                GroupLayout.DEFAULT_SIZE));
    }

    public void applyFilters() {
        instancesTable.clearSelection();
        instancesTable.loadFromDatabase();
    }

    public PipelineInstance selectedPipelineInstance() {
        return instancesTable.selectedPipelineInstance();
    }

    public InstancesTable instancesTable() {
        return instancesTable;
    }
}

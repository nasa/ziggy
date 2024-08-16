package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DELETE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.NEW;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.table.ZiggyTable;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;

/**
 * Edit/view all of the {@link ParameterSet}s for a pipeline or node. This panel is the one that
 * appears when the operator goes to the Pipelines panel, selects a pipeline, and selects a
 * parameter set or pipeline module from the resulting Edit Pipeline dialog box.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ParameterSetMapEditorPanel extends javax.swing.JPanel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParameterSetMapEditorPanel.class);

    private ParameterSetMapEditorListener mapListener;
    private ZiggyTable<ParameterSetAssignment> ziggyTable;
    private int selectedModelIndex = -1;
    private JPopupMenu contextMenu;

    private Map<String, ParameterSet> moduleParameterSetByName;
    private Map<String, ParameterSet> pipelineParameterSetByName;
    private Map<String, ParameterSet> editedParameterSetByName;

    private ParameterSetNamesTableModel paramSetMapTableModel;

    private final ParametersOperations parametersOperations = new ParametersOperations();

    public ParameterSetMapEditorPanel(Map<String, ParameterSet> moduleParameterSetByName,
        Map<String, ParameterSet> editedParameterSets) {
        this(moduleParameterSetByName, new HashMap<>(), editedParameterSets);
    }

    public ParameterSetMapEditorPanel(Map<String, ParameterSet> moduleParameterSetByName,
        Map<String, ParameterSet> pipelineParameterSetByName,
        Map<String, ParameterSet> editedParameterSetByName) {
        this.moduleParameterSetByName = moduleParameterSetByName;
        this.pipelineParameterSetByName = pipelineParameterSetByName;
        this.editedParameterSetByName = editedParameterSetByName;

        buildComponent();
    }

    private void buildComponent() {
        JPanel toolBar = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(NEW, "Add a new parameter set.", this::add),
            createButton(EDIT, "Edit this parameter set.", this::editParamValues));

        contextMenu = ZiggySwingUtils.createPopupMenu(createMenuItem(EDIT + DIALOG, this::edit),
            createMenuItem(DELETE, this::delete));
        paramSetMapTableModel = new ParameterSetNamesTableModel(moduleParameterSetByName,
            pipelineParameterSetByName);
        ziggyTable = new ZiggyTable<>(paramSetMapTableModel);
        JScrollPane parameterSets = new JScrollPane(ziggyTable.getTable());
        parameterSets.setPreferredSize(new Dimension(0, 100));

        GroupLayout dataPanelLayout = new GroupLayout(this);
        setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(toolBar)
            .addComponent(parameterSets));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(toolBar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(parameterSets));

        ziggyTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent evt) {
                if (evt.getClickCount() == 2) {
                    int tableRow = ziggyTable.rowAtPoint(evt.getPoint());
                    selectedModelIndex = ziggyTable.convertRowIndexToModel(tableRow);
                    edit(selectedModelIndex);
                }
            }

            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    contextMenu.show(ziggyTable.getTable(), e.getX(), e.getY());
                    int tableRow = ziggyTable.rowAtPoint(e.getPoint());
                    selectedModelIndex = ziggyTable.convertRowIndexToModel(tableRow);
                }
            }
        });
    }

    private void add(ActionEvent evt) {

        ParameterSet newParameterSet = ParameterSetSelectorDialog
            .selectParameterSet(SwingUtilities.getWindowAncestor(this));

        if (newParameterSet != null) {

            if (moduleParameterSetByName.containsKey(newParameterSet.getName())) {
                MessageUtils.showError(this, "A parameter set with name " + newParameterSet.getName()
                    + " already exists, use 'select' to change the existing instance");
            } else {
                moduleParameterSetByName.put(newParameterSet.getName(), newParameterSet);

                if (mapListener != null) {
                    mapListener.notifyMapChanged(this);
                }

                paramSetMapTableModel.update(moduleParameterSetByName, pipelineParameterSetByName);
            }
        }
    }

    private void editParamValues(ActionEvent evt) {
        int selectedRow = ziggyTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtils.showError(this, "No parameter set selected.");
        } else {
            edit(ziggyTable.convertRowIndexToModel(selectedRow));
        }
    }

    private void edit(ActionEvent evt) {
        edit(selectedModelIndex);
    }

    private void edit(int modelIndex) {
        String name = paramSetMapTableModel.getParamSetAtRow(modelIndex);
        if (name == null) {
            return;
        }

        new SwingWorker<ParameterSet, Void>() {
            @Override
            protected ParameterSet doInBackground() throws Exception {
                if (editedParameterSetByName.containsKey(name)) {
                    return editedParameterSetByName.get(name);
                }
                return parametersOperations().parameterSet(name);
            }

            @Override
            protected void done() {

                try {
                    ParameterSet parameters = get();
                    if (parameters == null) {
                        return;
                    }
                    ParameterSet newParameters = EditParametersDialog.editParameters(
                        SwingUtilities.getWindowAncestor(ParameterSetMapEditorPanel.this), name,
                        parameters);
                    if (newParameters != null) {
                        editedParameterSetByName.put(name, newParameters);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(
                        SwingUtilities.getWindowAncestor(ParameterSetMapEditorPanel.this), e);
                }
            }
        }.execute();
    }

    private void delete(ActionEvent evt) {
        String parameterSetName = ziggyTable.getContentAtViewRow(selectedModelIndex)
            .getAssignedName();
        moduleParameterSetByName.remove(parameterSetName);

        if (mapListener != null) {
            mapListener.notifyMapChanged(this);
        }

        paramSetMapTableModel.update(moduleParameterSetByName, pipelineParameterSetByName);
    }

    public ParameterSetMapEditorListener getMapListener() {
        return mapListener;
    }

    public void setMapListener(ParameterSetMapEditorListener mapListener) {
        this.mapListener = mapListener;
    }

    public Map<String, ParameterSet> getModuleParameterSetByName() {
        return moduleParameterSetByName;
    }

    private ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    private static class ParameterSetNamesTableModel
        extends AbstractZiggyTableModel<ParameterSetAssignment>
        implements ModelContentClass<ParameterSetAssignment> {

        private static final String[] COLUMN_NAMES = { "Name" };

        private final LinkedList<ParameterSetAssignment> paramSetAssignments = new LinkedList<>();

        public ParameterSetNamesTableModel(Map<String, ParameterSet> currentParameters,
            Map<String, ParameterSet> currentPipelineParameters) {
            update(currentParameters, currentPipelineParameters);
        }

        public void update(Map<String, ParameterSet> currentParameters,
            Map<String, ParameterSet> currentPipelineParameters) {

            paramSetAssignments.clear();

            // Current-level parameter sets.
            for (String parameterSetName : currentParameters.keySet()) {
                boolean assignedAtBothLevels = currentPipelineParameters
                    .containsKey(parameterSetName);
                paramSetAssignments
                    .add(new ParameterSetAssignment(parameterSetName, false, assignedAtBothLevels));
            }

            // Next-level up parameter sets, if any.
            for (String parameterSetName : currentPipelineParameters.keySet()) {
                if (currentParameters.containsKey(parameterSetName)) {
                    continue;
                }
                paramSetAssignments.add(new ParameterSetAssignment(parameterSetName, true, false));
            }

            fireTableDataChanged();
        }

        public String getParamSetAtRow(int rowIndex) {
            return paramSetAssignments.get(rowIndex).getAssignedName();
        }

        @Override
        public int getRowCount() {
            return paramSetAssignments.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            ParameterSetAssignment assignment = paramSetAssignments.get(rowIndex);
            String assignedName = assignment.getAssignedName();
            HtmlBuilder displayName = new HtmlBuilder();

            if (assignedName != null) {
                displayName.append(assignedName);
            } else {
                displayName.appendColor("--- Not set ---", "red");
            }

            if (assignment.isAssignedAtBothLevels()) {
                displayName.appendColor(" (ERROR: set at BOTH levels)", "red");
            } else if (assignment.isAssignedAtPipelineLevel()) {
                displayName.appendItalic(" (set at pipeline level)");
            }

            return switch (columnIndex) {
                case 0 -> displayName;
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<ParameterSetAssignment> tableModelContentClass() {
            return ParameterSetAssignment.class;
        }

        @Override
        public ParameterSetAssignment getContentAtRow(int row) {
            return paramSetAssignments.get(row);
        }
    }
}

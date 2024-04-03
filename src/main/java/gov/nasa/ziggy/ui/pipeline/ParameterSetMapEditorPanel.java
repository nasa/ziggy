package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ADD;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DIALOG;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REMOVE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SELECT;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createMenuItem;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.proxy.ParameterSetCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;
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
    private static final Logger log = LoggerFactory.getLogger(ParameterSetMapEditorPanel.class);

    private ParameterSetMapEditorListener mapListener;
    private ZiggyTable<ParameterSetAssignment> ziggyTable;
    private int selectedModelIndex = -1;

    private Map<ClassWrapper<ParametersInterface>, String> currentParameters;
    private Map<ClassWrapper<ParametersInterface>, String> currentPipelineParameters;
    private Map<String, ParametersInterface> editedParameterSets;

    private ParameterSetNamesTableModel paramSetMapTableModel;

    public ParameterSetMapEditorPanel(
        Map<ClassWrapper<ParametersInterface>, String> currentParameters,
        Map<ClassWrapper<ParametersInterface>, String> currentPipelineParameters,
        Map<String, ParametersInterface> editedParameterSets) {
        this.currentParameters = currentParameters;
        this.currentPipelineParameters = currentPipelineParameters;
        this.editedParameterSets = editedParameterSets;

        buildComponent();
    }

    private void buildComponent() {
        JPanel toolBar = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(ADD, "Add a new parameter set.", this::add),
            createButton(SELECT, "Select a different parameter set instance for this type.",
                this::selectParamSet),
            createButton(EDIT,
                "Shortcut to edit the values in this parameter set instance (same as editing the set in the Parameter Library).",
                this::editParamValues),
            createButton("Auto-assign",
                "Automatically assign a parameter set if there is only one available.",
                this::autoAssign));

        paramSetMapTableModel = new ParameterSetNamesTableModel(currentParameters,
            currentPipelineParameters);
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

        addMouseListener(ziggyTable);
        addMouseListener(ziggyTable.getTable(),
            ZiggySwingUtils.createPopupMenu(createMenuItem(SELECT + DIALOG, this::selectSelected),
                createMenuItem(EDIT + DIALOG, this::editSelected),
                createMenuItem(REMOVE, this::removeSelected)));
    }

    private void add(ActionEvent evt) {

        ParameterSet newParameterSet = ParameterSetSelectorDialog
            .selectParameterSet(SwingUtilities.getWindowAncestor(this));

        if (newParameterSet != null) {
            @SuppressWarnings("unchecked")
            Class<ParametersInterface> clazz = (Class<ParametersInterface>) newParameterSet.clazz();

            ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(clazz);

            if (currentParameters.containsKey(classWrapper)) {
                MessageUtil.showError(this, "A parameter set for " + clazz.getSimpleName()
                    + " already exists, use 'select' to change the existing instance");
            } else {
                currentParameters.put(classWrapper, newParameterSet.getName());

                if (mapListener != null) {
                    mapListener.notifyMapChanged(this);
                }

                paramSetMapTableModel.update(currentParameters, currentPipelineParameters);
            }
        }
    }

    private void selectParamSet(ActionEvent evt) {
        int selectedRow = ziggyTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No parameter set selected.");
        } else {
            select(ziggyTable.convertRowIndexToModel(selectedRow));
        }
    }

    private void select(int modelIndex) {
        ParameterSetAssignment paramSetAssignment = ziggyTable.getContentAtViewRow(modelIndex);

        if (paramSetAssignment.isAssignedAtPipelineLevel()) {
            MessageUtil.showError(this,
                "Already assigned at the pipeline level.  Remove that assignment first.");
            return;
        }

        ClassWrapper<ParametersInterface> type = paramSetAssignment.getType();
        boolean isDeleted = false;

        try {
            type.getClazz();
        } catch (Exception e) {
            isDeleted = true;
        }

        if (!isDeleted) {
            Class<? extends ParametersInterface> currentType = type.getClazz();
            ParameterSet newParameterSet = ParameterSetSelectorDialog
                .selectParameterSet(SwingUtilities.getWindowAncestor(this), currentType);

            if (newParameterSet != null) {
                String previouslyAssignedName = paramSetAssignment.getAssignedName();
                if (previouslyAssignedName == null || previouslyAssignedName != null
                    && !newParameterSet.getName().equals(previouslyAssignedName)) {
                    // changed, store the change
                    ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(
                        currentType);
                    currentParameters.put(classWrapper, newParameterSet.getName());

                    if (mapListener != null) {
                        mapListener.notifyMapChanged(this);
                    }

                    paramSetMapTableModel.update(currentParameters, currentPipelineParameters);
                }
            }
        } else {
            MessageUtil.showError(this,
                "Can't select a parameter set whose class has been deleted.");
        }
    }

    private void editParamValues(ActionEvent evt) {
        int selectedRow = ziggyTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No parameter set selected.");
        } else {
            edit(ziggyTable.convertRowIndexToModel(selectedRow));
        }
    }

    private void edit(int modelIndex) {
        String paramSetName = paramSetMapTableModel.getParamSetAtRow(modelIndex);

        String name = paramSetName;
        ParametersInterface parameters = null;
        if (paramSetName != null) {
            if (editedParameterSets.containsKey(paramSetName)) {
                parameters = editedParameterSets.get(paramSetName);
            } else {
                PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();
                ParameterSet latestParameterSet = pipelineOps
                    .retrieveLatestParameterSet(paramSetName);
                if (latestParameterSet.parametersClassDeleted()) {
                    MessageUtil.showError(this,
                        "Can't edit a parameter set whose class has been deleted.");
                    return;
                }
                parameters = latestParameterSet.parametersInstance();
            }
            try {
                ParametersInterface newParameters = EditParametersDialog
                    .editParameters(SwingUtilities.getWindowAncestor(this), name, parameters);
                if (newParameters != null) {
                    editedParameterSets.put(paramSetName, newParameters);
                }
            } catch (Throwable e) {
                MessageUtil.showError(SwingUtilities.getWindowAncestor(this), e);
            }
        }
    }

    private void autoAssign(ActionEvent evt) {
        ParameterSetCrudProxy crud = new ParameterSetCrudProxy();
        List<ParameterSet> allParamSets = crud.retrieveLatestVersions();
        LinkedList<ParameterSetAssignment> currentAssignments = paramSetMapTableModel
            .getParamSetAssignments();
        boolean changesMade = false;

        for (ParameterSetAssignment assignment : currentAssignments) {
            if (assignment.getAssignedName() == null) {
                ClassWrapper<ParametersInterface> type = assignment.getType();
                ParameterSet instance = null;
                int foundCount = 0;

                for (ParameterSet parameterSet : allParamSets) {
                    Class<?> clazz = null;

                    try {
                        clazz = parameterSet.clazz();
                    } catch (RuntimeException e) {
                        // ignore this parameter set
                    }

                    if (clazz != null && parameterSet.clazz().equals(type.getClazz())) {
                        instance = parameterSet;
                        foundCount++;
                    }
                }

                if (foundCount == 1) {
                    log.info("Found a match: " + instance.getName() + " for type: " + type);
                    currentParameters.put(type, instance.getName());
                    changesMade = true;
                }
            }
        }

        if (changesMade) {
            if (mapListener != null) {
                mapListener.notifyMapChanged(this);
            }

            paramSetMapTableModel.update(currentParameters, currentPipelineParameters);
        }
    }

    private void addMouseListener(final ZiggyTable<ParameterSetAssignment> ziggyTable) {
        ziggyTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent evt) {
                if (evt.getClickCount() == 2) {
                    int tableRow = ziggyTable.rowAtPoint(evt.getPoint());
                    selectedModelIndex = ziggyTable.convertRowIndexToModel(tableRow);
                    log.debug("[DC] selectedModelIndex={}", selectedModelIndex);

                    edit(selectedModelIndex);
                }
            }
        });
    }

    private void addMouseListener(final java.awt.Component component,
        final javax.swing.JPopupMenu menu) {
        component.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(component, e.getX(), e.getY());
                    int tableRow = ziggyTable.rowAtPoint(e.getPoint());
                    // windows bug? works ok on Linux/gtk. Here's a workaround:
                    if (tableRow == -1) {
                        tableRow = ziggyTable.getSelectedRow();
                    }
                    selectedModelIndex = ziggyTable.convertRowIndexToModel(tableRow);
                }
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(component, e.getX(), e.getY());
                }
            }
        });
    }

    private void selectSelected(ActionEvent evt) {
        select(selectedModelIndex);
    }

    private void editSelected(ActionEvent evt) {
        edit(selectedModelIndex);
    }

    private void removeSelected(ActionEvent evt) {
        ClassWrapper<ParametersInterface> type = ziggyTable.getContentAtViewRow(selectedModelIndex)
            .getType();
        currentParameters.remove(type);

        if (mapListener != null) {
            mapListener.notifyMapChanged(this);
        }

        paramSetMapTableModel.update(currentParameters, currentPipelineParameters);
    }

    public ParameterSetMapEditorListener getMapListener() {
        return mapListener;
    }

    public void setMapListener(ParameterSetMapEditorListener mapListener) {
        this.mapListener = mapListener;
    }

    public Map<ClassWrapper<ParametersInterface>, String> getParameterSetsMap() {
        return currentParameters;
    }

    private class ParameterSetNamesTableModel
        extends AbstractZiggyTableModel<ParameterSetAssignment>
        implements ModelContentClass<ParameterSetAssignment> {

        private static final String[] COLUMN_NAMES = { "Type", "Name" };

        private final LinkedList<ParameterSetAssignment> paramSetAssignments = new LinkedList<>();

        public ParameterSetNamesTableModel(
            Map<ClassWrapper<ParametersInterface>, String> currentParameters,
            Map<ClassWrapper<ParametersInterface>, String> currentPipelineParameters) {
            update(currentParameters, currentPipelineParameters);
        }

        /**
         * for each required param create a ParameterSetAssignment if reqd param exists in current
         * params, use that name if reqd param exists in current pipeline params, use that name with
         * '(pipeline)' if there are any left in current params (not reqd), add those
         */
        public void update(Map<ClassWrapper<ParametersInterface>, String> currentParameters,
            Map<ClassWrapper<ParametersInterface>, String> currentPipelineParameters) {

            paramSetAssignments.clear();
            Set<ClassWrapper<ParametersInterface>> types = new HashSet<>();

            // If there are any param types left over in current params (not required), add those.
            // This also covers the case where empty lists are passed in for required params and
            // current pipeline params (when using this model to edit pipeline params on the
            // EditPipelineDialog.
            for (ClassWrapper<ParametersInterface> currentParam : currentParameters.keySet()) {
                if (!types.contains(currentParam)) {
                    ParameterSetAssignment param = new ParameterSetAssignment(currentParam,
                        currentParameters.get(currentParam), false, false);
                    paramSetAssignments.add(param);
                }
            }

            fireTableDataChanged();
        }

        public String getParamSetAtRow(int rowIndex) {
            return paramSetAssignments.get(rowIndex).getAssignedName();
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
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            ParameterSetAssignment assignment = paramSetAssignments.get(rowIndex);
            ClassWrapper<ParametersInterface> assignmentType = assignment.getType();
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

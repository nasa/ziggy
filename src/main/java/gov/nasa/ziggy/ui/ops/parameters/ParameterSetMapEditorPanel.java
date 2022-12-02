package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.proxy.ParameterSetCrudProxy;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * Edit/view all of the {@link ParameterSet}s for a pipeline or node. This panel is the one that
 * appears when the operator goes to the Triggers tab, selects a trigger, and selects a parameter
 * set or pipeline module from the resulting Edit Trigger dialog box.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetMapEditorPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetMapEditorPanel.class);

    private ParameterSetMapEditorListener mapListener;
    private JButton autoAssignButton;
    private JMenuItem removeMenuItem;
    private JMenuItem editMenuItem;
    private JMenuItem selectMenuItem;
    private JPopupMenu tablePopupMenu;
    private JButton selectParamSetButton;
    private JButton addButton;
    private JButton editParamValuesButton;
    private JPanel buttonPanel;
    private ZTable paramSetMapTable;
    private JScrollPane tableScrollPane1;
    private JPanel tablePanel;
    private int selectedModelIndex = -1;

    private Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters = null;
    private Set<ClassWrapper<Parameters>> requiredParameters = null;
    private Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters = null;

    private ParameterSetNamesTableModel paramSetMapTableModel;

    /* For Jigloo use only */
    public ParameterSetMapEditorPanel() {
        initGUI();
    }

    public ParameterSetMapEditorPanel(
        Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters,
        Set<ClassWrapper<Parameters>> requiredParameters,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters) {
        this.currentParameters = currentParameters;
        this.requiredParameters = requiredParameters;
        this.currentPipelineParameters = currentPipelineParameters;

        initGUI();
    }

    private void initGUI() {
        try {
            GridBagLayout thisLayout = new GridBagLayout();
            setPreferredSize(new Dimension(400, 300));
            thisLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            thisLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7 };
            thisLayout.columnWeights = new double[] { 0.1 };
            thisLayout.columnWidths = new int[] { 7 };
            setLayout(thisLayout);
            this.add(getTablePanel(), new GridBagConstraints(0, 0, 1, 4, 0.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            this.add(getButtonPanel(), new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addButtonActionPerformed(ActionEvent evt) {
        log.debug("addButton.actionPerformed, event=" + evt);

        ParameterSet newParameterSet = ParameterSetSelectorDialog.selectParameterSet();

        if (newParameterSet != null) {
            @SuppressWarnings("unchecked")
            Class<? extends Parameters> clazz = (Class<? extends Parameters>) newParameterSet
                .getParameters()
                .getClazz();

            ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(clazz);

            if (currentParameters.containsKey(classWrapper)) {
                MessageUtil.showError(this, "A parameter set for " + clazz.getSimpleName()
                    + " already exists, use 'select' to change the existing instance");
            } else {
                currentParameters.put(classWrapper, newParameterSet.getName());

                if (mapListener != null) {
                    mapListener.notifyMapChanged(this);
                }

                paramSetMapTableModel.update(currentParameters, requiredParameters,
                    currentPipelineParameters);
            }
        }
    }

    private void selectMenuItemActionPerformed(ActionEvent evt) {
        log.debug("selectMenuItem.actionPerformed, event=" + evt);

        doSelect(selectedModelIndex);
    }

    private void selectParamSetButtonActionPerformed(ActionEvent evt) {
        log.debug("selectParamSetButton.actionPerformed, event=" + evt);

        int selectedRow = paramSetMapTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No parameter set selected");
        } else {
            int modelIndex = paramSetMapTable.convertRowIndexToModel(selectedRow);
            doSelect(modelIndex);
        }
    }

    /**
     * @param selectedRow
     */
    private void doSelect(int modelIndex) {
        ParameterSetAssignment paramSetAssignment = paramSetMapTableModel
            .getParamAssignmentAtRow(modelIndex);

        if (paramSetAssignment.isAssignedAtPipelineLevel()) {
            MessageUtil.showError(this,
                "Already assigned at the pipeline level.  Remove that assignment first.");
            return;
        }

        ClassWrapper<Parameters> type = paramSetAssignment.getType();
        boolean isDeleted = false;

        try {
            type.getClazz();
        } catch (Exception e) {
            isDeleted = true;
        }

        if (!isDeleted) {
            Class<? extends Parameters> currentType = type.getClazz();
            ParameterSet newParameterSet = ParameterSetSelectorDialog
                .selectParameterSet(currentType);

            if (newParameterSet != null) {
                ParameterSetName previouslyAssignedName = paramSetAssignment.getAssignedName();
                if (previouslyAssignedName == null || previouslyAssignedName != null
                    && !newParameterSet.getName().equals(previouslyAssignedName)) {
                    // changed, store the change
                    ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(currentType);
                    currentParameters.put(classWrapper, newParameterSet.getName());

                    if (mapListener != null) {
                        mapListener.notifyMapChanged(this);
                    }

                    paramSetMapTableModel.update(currentParameters, requiredParameters,
                        currentPipelineParameters);
                }
            }
        } else {
            MessageUtil.showError(this,
                "Can't select a parameter set whose class has been deleted.");
        }
    }

    private void editMenuItemActionPerformed(ActionEvent evt) {
        log.debug("editMenuItem.actionPerformed, event=" + evt);

        doEdit(selectedModelIndex);
    }

    private void editParamValuesButtonActionPerformed(ActionEvent evt) {
        log.debug("editParamValuesButton.actionPerformed, event=" + evt);

        int selectedRow = paramSetMapTable.getSelectedRow();

        if (selectedRow == -1) {
            MessageUtil.showError(this, "No parameter set selected");
        } else {
            int modelIndex = paramSetMapTable.convertRowIndexToModel(selectedRow);
            doEdit(modelIndex);
        }
    }

    /**
     * @param modelIndex
     */
    private void doEdit(int modelIndex) {
        ParameterSetName paramSetName = paramSetMapTableModel.getParamSetAtRow(modelIndex);

        if (paramSetName != null) {
            PipelineOperationsProxy pipelineOps = new PipelineOperationsProxy();

            ParameterSet latestParameterSet = pipelineOps.retrieveLatestParameterSet(paramSetName);

            if (!latestParameterSet.parametersClassDeleted()) {
                Parameters newParameters = EditParametersDialog.editParameters(latestParameterSet);

                if (newParameters != null) {
                    pipelineOps.updateParameterSet(latestParameterSet, newParameters,
                        latestParameterSet.getDescription(), false);
                }
            } else {
                MessageUtil.showError(this,
                    "Can't edit a parameter set whose class has been deleted.");
            }
        }
    }

    private void removeMenuItemActionPerformed(ActionEvent evt) {
        log.debug("removeMenuItem.actionPerformed, event=" + evt);

        ClassWrapper<Parameters> type = paramSetMapTableModel
            .getParamAssignmentAtRow(selectedModelIndex)
            .getType();
        currentParameters.remove(type);

        if (mapListener != null) {
            mapListener.notifyMapChanged(this);
        }

        paramSetMapTableModel.update(currentParameters, requiredParameters,
            currentPipelineParameters);
    }

    private void autoAssignButtonActionPerformed(ActionEvent evt) {
        log.debug("autoAssignButton.actionPerformed, event=" + evt);

        ParameterSetCrudProxy crud = new ParameterSetCrudProxy();
        List<ParameterSet> allParamSets = crud.retrieveLatestVersions();
        LinkedList<ParameterSetAssignment> currentAssignments = paramSetMapTableModel
            .getParamSetAssignments();
        boolean changesMade = false;

        for (ParameterSetAssignment assignment : currentAssignments) {
            if (assignment.getAssignedName() == null) {
                ClassWrapper<Parameters> type = assignment.getType();
                ParameterSet instance = null;
                int foundCount = 0;

                for (ParameterSet parameterSet : allParamSets) {
                    Class<?> clazz = null;

                    try {
                        clazz = parameterSet.getParameters().getClazz();
                    } catch (RuntimeException e) {
                        // ignore this parameter set
                    }

                    if (clazz != null
                        && parameterSet.getParameters().getClazz().equals(type.getClazz())) {
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

            paramSetMapTableModel.update(currentParameters, requiredParameters,
                currentPipelineParameters);
        }
    }

    public ParameterSetMapEditorListener getMapListener() {
        return mapListener;
    }

    public void setMapListener(ParameterSetMapEditorListener mapListener) {
        this.mapListener = mapListener;
    }

    public Map<ClassWrapper<Parameters>, ParameterSetName> getParameterSetsMap() {
        return currentParameters;
    }

    private JPanel getTablePanel() {
        if (tablePanel == null) {
            tablePanel = new JPanel();
            BorderLayout tablePanelLayout = new BorderLayout();
            tablePanel.setLayout(tablePanelLayout);
            tablePanel.add(getTableScrollPane1(), BorderLayout.CENTER);
        }
        return tablePanel;
    }

    private JScrollPane getTableScrollPane1() {
        if (tableScrollPane1 == null) {
            tableScrollPane1 = new JScrollPane();
            tableScrollPane1.setViewportView(getParamSetMapTable());
        }
        return tableScrollPane1;
    }

    private ZTable getParamSetMapTable() {
        if (paramSetMapTable == null) {
            paramSetMapTableModel = new ParameterSetNamesTableModel(currentParameters,
                requiredParameters, currentPipelineParameters);
            paramSetMapTable = new ZTable();
            paramSetMapTable.setRowShadingEnabled(true);
            paramSetMapTable.setModel(paramSetMapTableModel);
            paramSetMapTable.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent evt) {
                    tableMouseClicked(evt);
                }
            });
            setComponentPopupMenu(paramSetMapTable, getTablePopupMenu());
        }
        return paramSetMapTable;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(20);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getAddButton());
            buttonPanel.add(getSelectParamSetButton());
            buttonPanel.add(getEditParamValuesButton());
            buttonPanel.add(getAutoAssignButton());
        }
        return buttonPanel;
    }

    private JButton getSelectParamSetButton() {
        if (selectParamSetButton == null) {
            selectParamSetButton = new JButton();
            selectParamSetButton.setText("select");
            selectParamSetButton
                .setToolTipText("Select a different parameter set instance for this type");
            selectParamSetButton.addActionListener(evt -> selectParamSetButtonActionPerformed(evt));
        }
        return selectParamSetButton;
    }

    private JButton getEditParamValuesButton() {
        if (editParamValuesButton == null) {
            editParamValuesButton = new JButton();
            editParamValuesButton.setText("edit values");
            editParamValuesButton.setToolTipText(
                "Shortcut to edit the values in this parameter set instance (same as editing the set in the Parameter Library)");
            editParamValuesButton
                .addActionListener(evt -> editParamValuesButtonActionPerformed(evt));
        }
        return editParamValuesButton;
    }

    private JButton getAddButton() {
        if (addButton == null) {
            addButton = new JButton();
            addButton.setText("add");
            addButton.setToolTipText("Add a new parameter set");
            addButton.addActionListener(evt -> addButtonActionPerformed(evt));
        }
        return addButton;
    }

    private JPopupMenu getTablePopupMenu() {
        if (tablePopupMenu == null) {
            tablePopupMenu = new JPopupMenu();
            tablePopupMenu.add(getSelectMenuItem());
            tablePopupMenu.add(getEditMenuItem());
            tablePopupMenu.add(getRemoveMenuItem());
        }
        return tablePopupMenu;
    }

    private void setComponentPopupMenu(final java.awt.Component parent,
        final javax.swing.JPopupMenu menu) {
        parent.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                    int tableRow = paramSetMapTable.rowAtPoint(e.getPoint());
                    // windows bug? works ok on Linux/gtk. Here's a workaround:
                    if (tableRow == -1) {
                        tableRow = paramSetMapTable.getSelectedRow();
                    }
                    selectedModelIndex = paramSetMapTable.convertRowIndexToModel(tableRow);
                }
            }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    menu.show(parent, e.getX(), e.getY());
                }
            }
        });
    }

    private void tableMouseClicked(MouseEvent evt) {
        log.debug("tableMouseClicked(MouseEvent) - start");

        if (evt.getClickCount() == 2) {
            log.debug(
                "tableMouseClicked(MouseEvent) - [DOUBLE-CLICK] table.mouseClicked, event=" + evt);
            int tableRow = paramSetMapTable.rowAtPoint(evt.getPoint());
            selectedModelIndex = paramSetMapTable.convertRowIndexToModel(tableRow);
            log.debug("tableMouseClicked(MouseEvent) - [DC] table row =" + selectedModelIndex);

            doEdit(selectedModelIndex);
        }

        log.debug("tableMouseClicked(MouseEvent) - end");
    }

    private JMenuItem getSelectMenuItem() {
        if (selectMenuItem == null) {
            selectMenuItem = new JMenuItem();
            selectMenuItem.setText("Select Parameter Set...");
            selectMenuItem.addActionListener(evt -> selectMenuItemActionPerformed(evt));
        }
        return selectMenuItem;
    }

    private JMenuItem getEditMenuItem() {
        if (editMenuItem == null) {
            editMenuItem = new JMenuItem();
            editMenuItem.setText("Edit Parameter Values...");
            editMenuItem.addActionListener(evt -> editMenuItemActionPerformed(evt));
        }
        return editMenuItem;
    }

    private JMenuItem getRemoveMenuItem() {
        if (removeMenuItem == null) {
            removeMenuItem = new JMenuItem();
            removeMenuItem.setText("Remove Parameter Set");
            removeMenuItem.addActionListener(evt -> removeMenuItemActionPerformed(evt));
        }
        return removeMenuItem;
    }

    private JButton getAutoAssignButton() {
        if (autoAssignButton == null) {
            autoAssignButton = new JButton();
            autoAssignButton.setText("auto-assign");
            autoAssignButton.setToolTipText(
                "Automatically assign a parameter set if there is only one available");
            autoAssignButton.addActionListener(evt -> autoAssignButtonActionPerformed(evt));
        }
        return autoAssignButton;
    }
}

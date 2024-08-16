package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.util.HashSet;
import java.util.Set;

import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceFilter;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.LabelType;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstancesControlPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(InstancesControlPanel.class);

    private JCheckBox errorsStalledCheckBox;
    private JCheckBox initializedCheckBox;
    private JCheckBox processingCheckBox;
    private JCheckBox errorsRunningCheckBox;
    private JCheckBox completeCheckBox;
    private JTextField ageTextField;
    private JTextField instanceNameTextField;

    private final PipelineInstanceFilter filter;
    private InstancesPanel listener;

    public InstancesControlPanel(PipelineInstanceFilter filter) {
        this.filter = filter;
        buildComponent();
    }

    private void buildComponent() {
        setLayout(new BorderLayout());
        setOpaque(false);
        add(createDataPanel(), BorderLayout.CENTER);
    }

    private JPanel createDataPanel() {
        JButton clearFiltersButton = new JButton("Clear filters");
        clearFiltersButton.addActionListener(this::clear);

        JLabel showInstancesGroup = boldLabel("Show instances", LabelType.HEADING);
        JLabel startLabel = new JLabel("Started within the last");
        ageTextField = new JTextField("" + filter.getAgeDays());
        ageTextField.setColumns(4);
        ageTextField.addActionListener(this::search);
        JLabel daysLabel = new JLabel("days");

        JPanel dayButtonPanel = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton("1d", this::view1Day), createButton("7d", this::view7Days),
            createButton("14d", this::view14Days), createButton("30d", this::view30Days),
            createButton("90d", this::view90Days));

        Set<State> states = filter.getStates();
        JLabel limitStatesGroup = boldLabel("Limit states", LabelType.HEADING);

        // TODO Replace the following strings with a future enum.toString()
        initializedCheckBox = new JCheckBox("Initialized");
        initializedCheckBox
            .setSelected(states != null && states.contains(PipelineInstance.State.INITIALIZED));
        initializedCheckBox.addActionListener(this::search);

        processingCheckBox = new JCheckBox("Processing");
        processingCheckBox
            .setSelected(states != null && states.contains(PipelineInstance.State.PROCESSING));
        processingCheckBox.addActionListener(this::search);

        completeCheckBox = new JCheckBox("Completed");
        completeCheckBox
            .setSelected(states != null && states.contains(PipelineInstance.State.COMPLETED));
        completeCheckBox.addActionListener(this::search);

        errorsRunningCheckBox = new JCheckBox("Errors (running)");
        errorsRunningCheckBox
            .setSelected(states != null && states.contains(PipelineInstance.State.ERRORS_RUNNING));
        errorsRunningCheckBox.addActionListener(this::search);

        errorsStalledCheckBox = new JCheckBox("Errors (stalled)");
        errorsStalledCheckBox
            .setSelected(states != null && states.contains(PipelineInstance.State.ERRORS_STALLED));
        errorsStalledCheckBox.addActionListener(this::search);

        JLabel instanceNameGroup = boldLabel("Instance name", LabelType.HEADING);
        String name = filter.getNameContains();
        instanceNameTextField = new JTextField();
        instanceNameTextField.setColumns(15);
        instanceNameTextField.setText(name != null ? name : "");

        JButton searchButton = new JButton("Search");
        searchButton.addActionListener(this::search);

        JPanel filtersPanel = new JPanel();
        GroupLayout filtersPanelLayout = new GroupLayout(filtersPanel);
        filtersPanel.setLayout(filtersPanelLayout);

        filtersPanelLayout.setHorizontalGroup(filtersPanelLayout.createParallelGroup()
            .addComponent(clearFiltersButton)
            .addComponent(showInstancesGroup)
            .addGroup(filtersPanelLayout.createSequentialGroup()
                .addGap(ZiggyGuiConstants.INDENT)
                .addComponent(startLabel)
                .addGap(5) // PREFERRED is too big, no gap is too small
                .addComponent(ageTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE)
                .addGap(5) // PREFERRED is too big, no gap is too small
                .addComponent(daysLabel)
                .addPreferredGap(ComponentPlacement.RELATED)
                .addComponent(dayButtonPanel))
            .addComponent(limitStatesGroup)
            .addGroup(filtersPanelLayout.createSequentialGroup()
                .addGap(ZiggyGuiConstants.INDENT)
                .addGroup(filtersPanelLayout.createParallelGroup()
                    .addComponent(initializedCheckBox)
                    .addComponent(processingCheckBox)
                    .addComponent(completeCheckBox))
                .addPreferredGap(ComponentPlacement.UNRELATED)
                .addGroup(filtersPanelLayout.createParallelGroup()
                    .addComponent(errorsRunningCheckBox)
                    .addComponent(errorsStalledCheckBox)))
            .addComponent(instanceNameGroup)
            .addGroup(filtersPanelLayout.createSequentialGroup()
                .addGap(ZiggyGuiConstants.INDENT)
                .addGroup(filtersPanelLayout.createSequentialGroup()
                    .addComponent(instanceNameTextField, GroupLayout.PREFERRED_SIZE,
                        GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(ComponentPlacement.RELATED)
                    .addComponent(searchButton))));

        filtersPanelLayout.setVerticalGroup(filtersPanelLayout.createSequentialGroup()
            .addComponent(clearFiltersButton)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(showInstancesGroup)
            .addGroup(filtersPanelLayout.createParallelGroup(Alignment.CENTER)
                .addComponent(startLabel)
                .addComponent(ageTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE)
                .addComponent(daysLabel)
                .addComponent(dayButtonPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE))
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(limitStatesGroup)
            .addGroup(filtersPanelLayout.createParallelGroup()
                .addGroup(filtersPanelLayout.createSequentialGroup()
                    .addComponent(initializedCheckBox)
                    .addComponent(processingCheckBox)
                    .addComponent(completeCheckBox))
                .addGroup(filtersPanelLayout.createSequentialGroup()
                    .addComponent(errorsRunningCheckBox)
                    .addComponent(errorsStalledCheckBox)))
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(instanceNameGroup)
            .addGroup(filtersPanelLayout.createParallelGroup(Alignment.CENTER)
                .addComponent(instanceNameTextField, GroupLayout.PREFERRED_SIZE,
                    GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addComponent(searchButton, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                    GroupLayout.PREFERRED_SIZE)));

        return filtersPanel;
    }

    private void view1Day(ActionEvent evt) {
        ageTextField.setText("1");
        search(evt);
    }

    private void view7Days(ActionEvent evt) {
        ageTextField.setText("7");
        search(evt);
    }

    private void view14Days(ActionEvent evt) {
        ageTextField.setText("14");
        search(evt);
    }

    private void view30Days(ActionEvent evt) {
        ageTextField.setText("30");
        search(evt);
    }

    private void view90Days(ActionEvent evt) {
        ageTextField.setText("90");
        search(evt);
    }

    private void clear(ActionEvent evt) {
        ageTextField.setText(Integer.toString(PipelineInstanceFilter.DEFAULT_AGE));
        initializedCheckBox.setSelected(false);
        processingCheckBox.setSelected(false);
        completeCheckBox.setSelected(false);
        errorsRunningCheckBox.setSelected(false);
        errorsStalledCheckBox.setSelected(false);
        instanceNameTextField.setText("");
        updateFilter();
        if (listener != null) {
            listener.applyFilters();
        }
    }

    private void search(ActionEvent evt) {
        updateFilter();
        if (listener != null) {
            listener.applyFilters();
        }
    }

    private void updateFilter() {
        // Age
        try {
            int age = Integer.parseInt(ageTextField.getText());
            filter.setAgeDays(age);
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this,
                "Invalid age: " + ageTextField.getText() + ": " + e.getMessage(), "Error",
                JOptionPane.ERROR_MESSAGE);
        }

        // States
        Set<State> states = new HashSet<>();
        if (initializedCheckBox.isSelected()) {
            states.add(PipelineInstance.State.INITIALIZED);
        }
        if (processingCheckBox.isSelected()) {
            states.add(PipelineInstance.State.PROCESSING);
        }
        if (completeCheckBox.isSelected()) {
            states.add(PipelineInstance.State.COMPLETED);
        }
        if (errorsRunningCheckBox.isSelected()) {
            states.add(PipelineInstance.State.ERRORS_RUNNING);
        }
        if (errorsStalledCheckBox.isSelected()) {
            states.add(PipelineInstance.State.ERRORS_STALLED);
        }
        filter.setStates(states);

        // Name
        filter.setNameContains(instanceNameTextField.getText());

        log.debug("filter={}", filter);
    }

    public InstancesPanel getListener() {
        return listener;
    }

    public void setListener(InstancesPanel listener) {
        this.listener = listener;
    }
}

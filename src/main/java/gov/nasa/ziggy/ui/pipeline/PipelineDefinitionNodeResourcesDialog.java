package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.AWTEvent;
import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.ItemEvent;
import java.text.NumberFormat;
import java.util.function.Consumer;

import javax.swing.ButtonGroup;
import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.text.NumberFormatter;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.services.messages.WorkerResources;
import gov.nasa.ziggy.ui.util.HumanReadableHeapSize;
import gov.nasa.ziggy.ui.util.HumanReadableHeapSize.HeapSizeUnit;
import gov.nasa.ziggy.ui.util.ValidityTestingFormattedTextField;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * Dialog box for editing the worker count and heap size for a {@link PipelineDefinitionNode}.
 * <p>
 * The dialog box is initialized from the state of the {@link PipelineDefinitionNode}: if the node
 * uses default values for worker count or for heap size, that information is captured, the initial
 * values of worker count and heap size are likewise captured. The user can then perform the
 * following actions:
 * <ol>
 * <li>Switch between the default worker count and the user-specified worker count for the node in
 * question.
 * <li>Set the user-specified worker count for the node in question.
 * <li>Switch between the default heap size and the user-specified heap size for the node in
 * question.
 * <li>Set the user-specified heap size for the node in question.
 * <li>Capture any changes made by the user.
 * <li>Revert any changes made by the user. In this case, the node's state on exit will be identical
 * to its state on entry.
 * </ol>
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineDefinitionNodeResourcesDialog extends JDialog {

    private static final long serialVersionUID = 20230810L;

    private final PipelineDefinitionNode node;
    private final String pipelineDefinitionName;
    private final WorkerResources initialResources;
    private JCheckBox workerDefaultCheckBox;
    private JCheckBox heapSizeDefaultCheckBox;
    private JRadioButton mbUnitsButton;
    private JRadioButton gbUnitsButton;
    private JRadioButton tbUnitsButton;
    private ValidityTestingFormattedTextField workerCountTextArea;
    private ValidityTestingFormattedTextField heapSizeTextArea;
    private ButtonGroup unitButtonGroup;
    private int workerCountCurrentUserValue;
    private int heapSizeMbCurrentUserValue;
    private JButton closeButton;
    private JButton cancelButton;

    /**
     * Whenever a validity check is performed, set the correct state for the close button.
     */
    private Consumer<Boolean> validityCheck = valid -> setCloseButtonState();

    public PipelineDefinitionNodeResourcesDialog(Window owner, String pipelineDefinitionName,
        PipelineDefinitionNode node) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.pipelineDefinitionName = pipelineDefinitionName;
        this.node = node;

        initialResources = node.workerResources();
        workerCountCurrentUserValue = initialResources.getMaxWorkerCount();
        heapSizeMbCurrentUserValue = initialResources.getHeapSizeMb();

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit worker resources");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        closeButton = createCloseButton();
        cancelButton = createCancelButton();
        getContentPane().add(createButtonPanel(cancelButton, closeButton), BorderLayout.SOUTH);

        initializeFieldState();

        pack();
    }

    private JPanel createDataPanel() {
        JLabel pipeline = boldLabel("Pipeline");
        JLabel pipelineText = new JLabel(pipelineDefinitionName);

        JLabel module = boldLabel("Module");
        JLabel moduleText = new JLabel(node.getModuleName());

        JLabel maxWorkers = boldLabel("Maximum workers");
        workerCountTextArea = createWorkerCountTextArea();
        workerDefaultCheckBox = createWorkerDefaultCheckBox();

        JLabel maxHeapSize = boldLabel("Maximum heap size");
        heapSizeTextArea = createHeapSizeTextArea();
        heapSizeDefaultCheckBox = createHeapSizeDefaultCheckBox();

        unitButtonGroup = new ButtonGroup();
        mbUnitsButton = new JRadioButton("MB");
        mbUnitsButton.setToolTipText("Set heap size units to megabytes.");
        unitButtonGroup.add(mbUnitsButton);
        gbUnitsButton = new JRadioButton("GB");
        gbUnitsButton.setToolTipText("Set heap size units to gigabytes.");
        unitButtonGroup.add(gbUnitsButton);
        tbUnitsButton = new JRadioButton("TB");
        tbUnitsButton.setToolTipText("Set heap size units to terabytes.");
        unitButtonGroup.add(tbUnitsButton);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(pipeline)
            .addComponent(pipelineText)
            .addComponent(module)
            .addComponent(moduleText)
            .addComponent(maxWorkers)
            .addComponent(workerCountTextArea, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(workerDefaultCheckBox)
            .addComponent(maxHeapSize)
            .addGroup(dataPanelLayout.createSequentialGroup()
                .addComponent(heapSizeTextArea, GroupLayout.PREFERRED_SIZE,
                    GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(ComponentPlacement.UNRELATED)
                .addComponent(mbUnitsButton)
                .addPreferredGap(ComponentPlacement.RELATED)
                .addComponent(gbUnitsButton)
                .addPreferredGap(ComponentPlacement.RELATED)
                .addComponent(tbUnitsButton))
            .addComponent(heapSizeDefaultCheckBox));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(pipeline)
            .addComponent(pipelineText)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(module)
            .addComponent(moduleText)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(maxWorkers)
            .addComponent(workerCountTextArea, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(workerDefaultCheckBox)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(maxHeapSize)
            .addGroup(dataPanelLayout.createParallelGroup(GroupLayout.Alignment.CENTER)
                .addComponent(heapSizeTextArea, GroupLayout.PREFERRED_SIZE,
                    GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addComponent(mbUnitsButton)
                .addComponent(gbUnitsButton)
                .addComponent(tbUnitsButton))
            .addComponent(heapSizeDefaultCheckBox));

        return dataPanel;
    }

    /**
     * Configures the "Close" button, which captures the user's changes and exits the dialog box.
     */
    private JButton createCloseButton() {
        JButton closeButton = ZiggySwingUtils.createButton(CLOSE,
            htmlBuilder("Close this dialog box.").appendBreak()
                .append(
                    "Your worker resource parameter changes won't be saved until you hit Save on the edit pipeline dialog.")
                .toString(),
            this::updateWorkerResources);

        // NB: the FocusListener is needed because of the way that the field values are
        // validated. When one of the ValidityTestingFormattedTextBox instances loses the
        // focus, the resulting event causes the validity of that box to become false for
        // a blink, which causes the close button to become disabled for a
        // blink; as a result, the buttons are disabled when the action listener would tell
        // them to take their actions, and the user has to hit the button again to get it to
        // take its action. By performing the action when the button gains focus, we get
        // back to needing only one button push to get the focus and perform the action.
        closeButton.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                if (closeButton.isEnabled()) {
                    updateWorkerResources(e);
                }
            }
        });

        return closeButton;
    }

    /**
     * Capture the current state of worker resource parameters in the relevant
     * {@link PipelineDefinitionNode} instance and close the dialog box.
     */
    private void updateWorkerResources(AWTEvent evt) {
        Integer finalWorkerCount = null;
        if (!workerDefaultCheckBox.isSelected()) {
            finalWorkerCount = (Integer) workerCountTextArea.getValue();
        }
        Integer finalHeapSizeMb = null;
        if (!heapSizeDefaultCheckBox.isSelected()) {
            finalHeapSizeMb = heapSizeMbFromTextField();
        }
        node.applyWorkerResources(new WorkerResources(finalWorkerCount, finalHeapSizeMb));
        dispose();
    }

    /**
     * Converts the combined heap size text field and heap size units button into a value for heap
     * size in MB.
     */
    private int heapSizeMbFromTextField() {
        double heapSize = (double) heapSizeTextArea.getValue();
        HeapSizeUnit heapSizeUnit = null;
        if (mbUnitsButton.isSelected()) {
            heapSizeUnit = HeapSizeUnit.MB;
        } else if (gbUnitsButton.isSelected()) {
            heapSizeUnit = HeapSizeUnit.GB;
        } else if (tbUnitsButton.isSelected()) {
            heapSizeUnit = HeapSizeUnit.TB;
        }
        return new HumanReadableHeapSize((float) heapSize, heapSizeUnit).heapSizeMb();
    }

    /**
     * Enables or disables the close button based on whether there are valid values for both the
     * worker count and the heap size. Either of these values is permitted to be invalid if the user
     * has selected the default value for that parameter.
     */
    private void setCloseButtonState() {
        closeButton
            .setEnabled((workerDefaultCheckBox.isSelected() || workerCountTextArea.isValidState())
                && (heapSizeDefaultCheckBox.isSelected() || heapSizeTextArea.isValidState()));
    }

    /**
     * Creates the cancel button, which reverts the worker parameters to original values and exits
     * the dialog box.
     */
    private JButton createCancelButton() {
        JButton cancelButton = ZiggySwingUtils.createButton(CANCEL,
            "Revert worker resource parameters to original values and close this dialog box.",
            this::revertWorkerResources);
        cancelButton.setEnabled(true);
        return cancelButton;
    }

    /**
     * Reverts the worker resources to their initial values and exits.
     */
    private void revertWorkerResources(ActionEvent evt) {
        node.applyWorkerResources(initialResources);
        dispose();
    }

    /**
     * Creates the {@link #workerCountTextArea}. This text area allows the user to set the number of
     * workers from 1 to the number of available processors. A validity check method is applied that
     * disables the close button when there's an invalid value in this text field.
     */
    private ValidityTestingFormattedTextField createWorkerCountTextArea() {
        NumberFormatter formatter = new NumberFormatter(NumberFormat.getInstance());
        formatter.setValueClass(Integer.class);
        formatter.setMinimum(1);
        formatter.setMaximum(Runtime.getRuntime().availableProcessors());
        ValidityTestingFormattedTextField workerCountTextArea = new ValidityTestingFormattedTextField(
            formatter);
        workerCountTextArea.setEmptyIsValid(false);
        workerCountTextArea.setColumns(10);
        workerCountTextArea
            .setToolTipText(htmlBuilder("Set the maximum number of worker processes.").appendBreak()
                .append("Must be between 1 and number of cores on your system (")
                .append(Runtime.getRuntime().availableProcessors())
                .append("), inclusive.")
                .toString());
        workerCountTextArea.setExecuteOnValidityCheck(validityCheck);
        return workerCountTextArea;
    }

    /**
     * Creates the {@link #workerDefaultCheckBox}. The initial value of the check box is set, a
     * listener is added that runs when the box state changes, and that listener is executed once to
     * correctly set the worker count field, if needed.
     *
     * @return
     */
    private JCheckBox createWorkerDefaultCheckBox() {
        JCheckBox workerDefaultCheckBox = new JCheckBox(
            "Default (" + WorkerResources.getDefaultResources().getMaxWorkerCount() + ")");
        workerDefaultCheckBox.setSelected(initialResources.maxWorkerCountIsDefault());
        workerDefaultCheckBox.setToolTipText("Use the pipeline default worker count.");
        workerDefaultCheckBox.addItemListener(this::workerCheckBoxChanged);
        return workerDefaultCheckBox;
    }

    /**
     * Checks the state of the worker default check box. If the check box is selected, the text
     * field for the worker count is disabled and the max worker count on the
     * {@link PipelineDefinitionNode} is set to zero, indicating that the default value should be
     * used; the user current value for the worker count is captured if the text field is valid.
     */
    private void workerCheckBoxChanged(ItemEvent evt) {
        workerCountTextArea.setEnabled(!workerDefaultCheckBox.isSelected());

        if (workerCountTextArea.isEnabled()) {
            // Switching from the default value to a user-set one.
            workerCountTextArea.setValue(workerCountCurrentUserValue);
        } else {

            // Switching from the user-set value to the default.
            if (workerCountTextArea.isValidState()) {
                workerCountCurrentUserValue = (int) workerCountTextArea.getValue();
            } else {
                // Force the text field into a valid state. This is a kind of kludgey way
                // to do it, but I haven't been able to figure out any way for Java to
                // do it automatically when the text field is disabled.
                workerCountTextArea.setValue(1);
            }
            workerCountTextArea.setValue(null);
        }
    }

    /**
     * Creates the {@link #workerCountTextArea}. This text area allows users to enter integer values
     * for the heap size from 1 to 1000 (there are also 3 buttons that set the units to MB, GB, or
     * TB). A validity check is applied such that when the text field is invalid the close button is
     * disabled.
     */
    private ValidityTestingFormattedTextField createHeapSizeTextArea() {
        NumberFormatter formatter = new NumberFormatter(NumberFormat.getInstance());
        formatter.setValueClass(Double.class);
        formatter.setMinimum(1.0);
        formatter.setMaximum(1000.0);
        ValidityTestingFormattedTextField heapSizeTextArea = new ValidityTestingFormattedTextField(
            formatter);
        heapSizeTextArea.setEmptyIsValid(false);
        heapSizeTextArea.setColumns(10);
        heapSizeTextArea.setToolTipText(
            htmlBuilder("Set the maximum Java heap size shared by all workers.").appendBreak()
                .append("Must be between 1 and 1000, inclusive.")
                .toString());
        heapSizeTextArea.setExecuteOnValidityCheck(validityCheck);
        return heapSizeTextArea;
    }

    /**
     * Creates the {@link #heapSizeDefaultCheckBox}. The initial value of the check box is set, a
     * listener is added that runs when the box state changes, and that listener is executed once to
     * correctly set the heap size text field, if needed.
     */
    private JCheckBox createHeapSizeDefaultCheckBox() {
        JCheckBox heapSizeDefaultCheckBox = new JCheckBox(
            "Default (" + WorkerResources.getDefaultResources().humanReadableHeapSize() + ")");
        heapSizeDefaultCheckBox.setSelected(initialResources.heapSizeIsDefault());
        heapSizeDefaultCheckBox.setToolTipText("Use the pipeline default heap size.");
        heapSizeDefaultCheckBox.addItemListener(this::heapSizeCheckBoxChanged);
        return heapSizeDefaultCheckBox;
    }

    /**
     * Take necessary actions when the default heap size box changes state. If the box goes from
     * disabled to enabled, the {@link PipelineDefinitionNode} value of the heap size is set to a
     * value that indicates that the default should be used, the text field and the unit buttons are
     * disabled, and the current value of the text box is captured if it is valid. If the box goes
     * from disabled to enabled, the text field and unit buttons are enabled and the current user
     * value of the heap is entered into the text field.
     */
    private void heapSizeCheckBoxChanged(ItemEvent evt) {
        heapSizeTextArea.setEnabled(!heapSizeDefaultCheckBox.isSelected());

        if (heapSizeTextArea.isEnabled()) {
            // Switching from the default heap size to the user-set one.
            mbUnitsButton.setEnabled(true);
            gbUnitsButton.setEnabled(true);
            tbUnitsButton.setEnabled(true);
            setHeapSizeTextFromCurrentUserValue();
        } else {

            // Switching from the user-set heap size to the default value.
            if (heapSizeTextArea.isValidState()) {
                heapSizeMbCurrentUserValue = heapSizeMbFromTextField();
            } else {
                // Force the text field into a valid state. This is a kind of kludgey way
                // to do it, but I haven't been able to figure out any way for Java to
                // do it automatically when the text field is disabled.
                heapSizeTextArea.setValue(1.0);
            }

            heapSizeTextArea.setValue(null);
            mbUnitsButton.setEnabled(false);
            gbUnitsButton.setEnabled(false);
            tbUnitsButton.setEnabled(false);
        }
    }

    /**
     * Converts the current user value of heap size (in MB) into a human-readable form and a unit,
     * which are then applied to the heap size text field and heap size unit buttons.
     */
    private void setHeapSizeTextFromCurrentUserValue() {
        HumanReadableHeapSize humanReadableHeapSize = new HumanReadableHeapSize(
            heapSizeMbCurrentUserValue);
        switch (humanReadableHeapSize.getHeapSizeUnit()) {
            case MB:
                mbUnitsButton.setSelected(true);
                break;
            case GB:
                gbUnitsButton.setSelected(true);
                break;
            case TB:
                tbUnitsButton.setSelected(true);
                break;
        }
        heapSizeTextArea.setValue((double) humanReadableHeapSize.getHumanReadableHeapSize());
    }

    /**
     * Ensures that components have the correct state per the initial content.
     */
    private void initializeFieldState() {
        workerCheckBoxChanged(null);
        heapSizeCheckBoxChanged(null);
    }
}

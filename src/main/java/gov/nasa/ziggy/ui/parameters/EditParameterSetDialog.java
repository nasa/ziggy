package gov.nasa.ziggy.ui.parameters;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.RESTORE_DEFAULTS;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.swing.BorderFactory;
import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingWorker;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.ui.util.MessageUtils;
import gov.nasa.ziggy.ui.util.PropertySheetHelper;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.util.PipelineException;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class EditParameterSetDialog extends javax.swing.JDialog {
    private enum DismissOption {
        SAVE, CLOSE
    }

    private final ParameterSet parameterSet;
    private PropertySheetPanel propertySheetPanel;

    private boolean cancelled;

    private final ParametersOperations parametersOperations = new ParametersOperations();

    public EditParameterSetDialog(Window owner, ParameterSet parameterSet) {
        this(owner, parameterSet, DismissOption.SAVE);
    }

    public EditParameterSetDialog(Window owner, ParameterSet parameterSet,
        DismissOption dismissOption) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.parameterSet = parameterSet;

        buildComponent(dismissOption);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(DismissOption dismissOption) {
        setTitle("Edit parameter set");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(dismissOption == DismissOption.SAVE ? createButton(SAVE, this::save)
                : createButton(CLOSE, this::close), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);
        populatePropertySheetPanel();

        pack();
    }

    private JPanel createDataPanel() {
        JLabel name = boldLabel("Name");
        JLabel nameTextField = new JLabel(parameterSet.getName());

        JLabel version = boldLabel("Version");
        JLabel versionTextField = new JLabel(Integer.toString(parameterSet.getVersion()));

        JLabel description = boldLabel("Description");
        JTextArea descriptionTextArea = new JTextArea(parameterSet.getDescription());
        descriptionTextArea.setEditable(false);
        JScrollPane descriptionScrollPane = new JScrollPane(descriptionTextArea);
        descriptionScrollPane.setBorder(BorderFactory.createEmptyBorder());

        JPanel buttonPanel = ZiggySwingUtils.createButtonPanel(ButtonPanelContext.TOOL_BAR,
            ZiggySwingUtils.createButton(RESTORE_DEFAULTS, this::defaults));

        JLabel parameters = boldLabel("Parameters");
        propertySheetPanel = new PropertySheetPanel();

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(name)
            .addComponent(nameTextField)
            .addComponent(version)
            .addComponent(versionTextField)
            .addComponent(description)
            .addComponent(descriptionScrollPane)
            .addComponent(parameters)
            .addComponent(buttonPanel)
            .addComponent(propertySheetPanel));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(version)
            .addComponent(versionTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(description)
            .addComponent(descriptionScrollPane)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(parameters)
            .addComponent(buttonPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(propertySheetPanel));

        return dataPanel;
    }

    private void defaults(ActionEvent evt) {
        propertySheetPanel.readFromObject(parameterSet);
    }

    private void save(ActionEvent evt) {
        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                parametersOperations().updateParameterSet(parameterSet, getParameterSet(), false);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    setVisible(false);
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(getRootPane(), e);
                }
            }
        }.execute();
    }

    public ParameterSet getParameterSet() throws PipelineException {
        if (cancelled) {
            return null;
        }
        ParameterSet currentParameterSet = new ParameterSet(parameterSet);
        propertySheetPanel.writeToObject(currentParameterSet.getParameters());
        return currentParameterSet;
    }

    private void close(ActionEvent actionevent1) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    private void populatePropertySheetPanel() {
        if (parameterSet != null) {
            try {
                PropertySheetHelper.populatePropertySheet(parameterSet, propertySheetPanel);
            } catch (Exception e) {
                throw new PipelineException("Failed to populate property sheet from parameters", e);
            }
        }
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public static ParameterSet editParameterSet(Window owner, ParameterSet parameterSet) {
        EditParameterSetDialog dialog = new EditParameterSetDialog(owner, parameterSet,
            DismissOption.CLOSE);
        dialog.setVisible(true);
        return dialog.getParameterSet();
    }

    private ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    public static void main(String[] args) {
        ParameterSet parameters = new ParameterSet("test");
        parameters.setParameters(
            Set.of(new Parameter("c", "z"), new Parameter("b", "y"), new Parameter("a", "x")));
        ZiggySwingUtils.displayTestDialog(new EditParameterSetDialog(null, parameters));
    }
}

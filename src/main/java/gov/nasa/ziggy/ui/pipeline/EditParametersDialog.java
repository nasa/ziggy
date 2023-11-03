package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;

/**
 * Dialog for viewing/editing a {@link Parameters} object.
 * <p>
 * This dialog box is generated from the Edit pipeline popup that is launched from the Pipelines
 * panel.
 * <p>
 * The dialog box presents the user with two options: "Close" and "Cancel." If the user chooses
 * cancel, their changes to the parameter set are abandoned. If the user chooses "Close," the
 * changes are retained locally (i.e., in the console), but not yet saved. If the user chooses
 * "Save" from the edit pipelines dialog box, any such retained changes are saved to the database at
 * that time; if the user chooses "Cancel" from the pipelines dialog box, changes that had been
 * retained in the console are lost, as are all other changes made since the edit pipelines dialog
 * box appeared.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class EditParametersDialog extends javax.swing.JDialog {
    private EditParametersPanel parametersPanel;
    private final String parameterSetName;
    private final ParametersInterface parameters;
    private boolean cancelled = false;

    public EditParametersDialog(Window owner, String parameterSetName,
        ParametersInterface parameters) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.parameterSetName = parameterSetName;
        this.parameters = parameters;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit parameter set");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(CLOSE, this::close), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {
        JLabel parameterSet = boldLabel("Parameter set");
        JLabel parameterSetTextField = new JLabel(parameterSetName);
        JLabel parametersLabel = boldLabel("Parameters");
        parametersPanel = new EditParametersPanel(parameters);

        // Enclose the panel in another panel to encourage the panel to fill the space.
        JPanel growPanel = new JPanel(new BorderLayout());
        growPanel.add(parametersPanel);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(parameterSet)
            .addComponent(parameterSetTextField)
            .addComponent(parametersLabel)
            .addComponent(growPanel));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(parameterSet)
            .addComponent(parameterSetTextField)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(parametersLabel)
            .addComponent(growPanel));

        return dataPanel;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    public static ParametersInterface editParameters(Window owner, String parameterSetName,
        ParametersInterface parameters) {
        EditParametersDialog dialog = new EditParametersDialog(owner, parameterSetName, parameters);
        dialog.setVisible(true);

        if (dialog.cancelled) {
            return null;
        }
        return dialog.parametersPanel.getParameters();
    }
}

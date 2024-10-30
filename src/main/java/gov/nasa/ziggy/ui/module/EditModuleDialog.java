package gov.nasa.ziggy.ui.module;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.concurrent.ExecutionException;

import javax.swing.BorderFactory;
import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.MessageUtils;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class EditModuleDialog extends javax.swing.JDialog {
    private static final long serialVersionUID = 20241002L;
    private static final Logger log = LoggerFactory.getLogger(EditModuleDialog.class);

    private PipelineModuleDefinition module;
    private PipelineModuleExecutionResources executionResources;

    private JTextField exeTimeoutText;
    private JTextField minMemoryText;

    private boolean cancelled = false;

    private final PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();

    public EditModuleDialog(Window owner, PipelineModuleDefinition module) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.module = module;

        buildComponent();
        updateExecutionResources();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Edit module");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(SAVE, this::save), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel createDataPanel() {
        JLabel name = boldLabel("Name");
        JLabel nameText = new JLabel(module.getName());

        JLabel desc = boldLabel("Description");
        JTextArea descText = new JTextArea(module.getDescription());
        descText.setLineWrap(true);
        descText.setWrapStyleWord(true);
        descText.setEditable(false);
        JScrollPane descScrollPane = new JScrollPane(descText);
        descScrollPane.setBorder(BorderFactory.createEmptyBorder());

        JLabel implementingClass = boldLabel("Implementing class");
        JLabel implementingClassText = new JLabel(module.getPipelineModuleClass().toString());

        JLabel exeTimeout = boldLabel("Executable timeout (seconds)");
        exeTimeoutText = new JTextField(ZiggyGuiConstants.LOADING);
        exeTimeoutText.setColumns(15);

        JLabel minMemory = boldLabel("Minimum memory (MB)");
        minMemoryText = new JTextField(ZiggyGuiConstants.LOADING);
        minMemoryText.setColumns(15);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(name)
            .addComponent(nameText)
            .addComponent(desc)
            .addComponent(descScrollPane)
            .addComponent(implementingClass)
            .addComponent(implementingClassText)
            .addComponent(exeTimeout)
            .addComponent(exeTimeoutText, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(minMemory)
            .addComponent(minMemoryText, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(desc)
            .addComponent(descScrollPane)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(implementingClass)
            .addComponent(implementingClassText)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(exeTimeout)
            .addComponent(exeTimeoutText, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.RELATED)
            .addComponent(minMemory)
            .addComponent(minMemoryText, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));
        return dataPanel;
    }

    private void save(ActionEvent evt) {

        new SwingWorker<Void, Void>() {

            @Override
            protected Void doInBackground() throws Exception {
                executionResources.setExeTimeoutSeconds(toInt(exeTimeoutText.getText(), 0));
                executionResources.setMinMemoryMegabytes(toInt(minMemoryText.getText(), 0));
                executionResources = pipelineModuleDefinitionOperations().merge(executionResources);

                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    setVisible(false);
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(EditModuleDialog.this, e);
                }
            }
        }.execute();
    }

    /**
     * Convert the specified String to an int and return the defaultValue if the conversion fails
     */
    private int toInt(String s, int defaultValue) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    private void updateExecutionResources() {
        new SwingWorker<PipelineModuleExecutionResources, Void>() {
            @Override
            protected PipelineModuleExecutionResources doInBackground() throws Exception {
                return pipelineModuleDefinitionOperations()
                    .pipelineModuleExecutionResources(module);
            }

            @Override
            protected void done() {
                try {
                    executionResources = get();
                    exeTimeoutText
                        .setText(Integer.toString(executionResources.getExeTimeoutSeconds()));
                    minMemoryText
                        .setText(Integer.toString(executionResources.getMinMemoryMegabytes()));
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Could not load pipeline module execution resources", e);
                }
            }
        }.execute();
    }

    public boolean isCancelled() {
        return cancelled;
    }

    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }
}

package gov.nasa.ziggy.ui.config.pipeline;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * Allows editing a new or existing {@link PipelineDefinition}, including selecting an optional
 * {@link Parameters} class and editing its properties.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineEditDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(PipelineEditDialog.class);

    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JTextField nameText;
    private JTextArea descText;
    private JScrollPane descScrollPane;
    private JLabel metadataLabel;
    private JLabel descLabel;
    private JLabel nameLabel;
    private JButton cancelButton;
    private JButton saveButton;

    private final PipelineDefinition pipelineDef;

    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;
    private JTextField groupNameTextField;
    private JLabel groupLabel;

    public PipelineEditDialog(JFrame frame, PipelineDefinition pipeline) {
        super(frame, "Edit Pipeline " + pipeline.getName(), true);
        pipelineDef = pipeline;

        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();
        initGUI();
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            this.setSize(500, 300);
        } catch (Exception e) {
            log.error("initGUI()", e);

            e.printStackTrace();
        }

        log.debug("initGUI() - end");
    }

    private void saveButtonActionPerformed() {
        log.debug("saveButtonActionPerformed(ActionEvent) - start");

        try {
            PipelineDefinition newPipelineDef;

            if (pipelineDef.isLocked()) {
                // pipeline definition is locked, so make a new version
                newPipelineDef = pipelineDef.newVersion();
            } else {
                // just update the existing instance
                newPipelineDef = pipelineDef;
            }

            // newPipelineDef.setName(nameText.getText());
            newPipelineDef.setDescription(descText.getText());

            pipelineDefinitionCrud.save(newPipelineDef);
            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("saveButtonActionPerformed(ActionEvent) - end");
    }

    private void cancelButtonActionPerformed() {
        log.debug("cancelButtonActionPerformed(ActionEvent) - start");

        setVisible(false);

        log.debug("cancelButtonActionPerformed(ActionEvent) - end");
    }

    private JPanel getDataPanel() {
        log.debug("getDataPanel() - start");

        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getNameLabel(),
                new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getNameText(),
                new GridBagConstraints(2, 1, 9, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getDescLabel(),
                new GridBagConstraints(1, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getDescScrollPane(),
                new GridBagConstraints(2, 3, 9, 3, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getMetadataLabel(),
                new GridBagConstraints(1, 7, 11, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getGroupLabel(),
                new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getGroupNameTextField(),
                new GridBagConstraints(2, 2, 9, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }

        log.debug("getDataPanel() - end");
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        log.debug("getButtonPanel() - start");

        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getSaveButton());
            buttonPanel.add(getCancelButton());
        }

        log.debug("getButtonPanel() - end");
        return buttonPanel;
    }

    private JButton getSaveButton() {
        log.debug("getSaveButton() - start");

        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("Save Changes");
            saveButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                saveButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getSaveButton() - end");
        return saveButton;
    }

    private JButton getCancelButton() {
        log.debug("getCancelButton() - start");

        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                cancelButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getCancelButton() - end");
        return cancelButton;
    }

    private JLabel getNameLabel() {
        log.debug("getNameLabel() - start");

        if (nameLabel == null) {
            nameLabel = new JLabel();
            nameLabel.setText("Name");
        }

        log.debug("getNameLabel() - end");
        return nameLabel;
    }

    private JTextField getNameText() {
        log.debug("getNameText() - start");

        if (nameText == null) {
            nameText = new JTextField(pipelineDef.getName().toString());
            nameText.setEditable(false);
        }

        log.debug("getNameText() - end");
        return nameText;
    }

    private JLabel getDescLabel() {
        log.debug("getDescLabel() - start");

        if (descLabel == null) {
            descLabel = new JLabel();
            descLabel.setText("Desc.");
        }

        log.debug("getDescLabel() - end");
        return descLabel;
    }

    private JScrollPane getDescScrollPane() {
        log.debug("getDescScrollPane() - start");

        if (descScrollPane == null) {
            descScrollPane = new JScrollPane();
            descScrollPane.setViewportView(getDescText());
        }

        log.debug("getDescScrollPane() - end");
        return descScrollPane;
    }

    private JTextArea getDescText() {
        log.debug("getDescText() - start");

        if (descText == null) {
            descText = new JTextArea();
            descText.setText(pipelineDef.getDescription());
        }

        log.debug("getDescText() - end");
        return descText;
    }

    private JLabel getMetadataLabel() {
        log.debug("getMetadataLabel() - start");

        if (metadataLabel == null) {
            metadataLabel = new JLabel();
            metadataLabel.setText("ID: xxx Created 1/1/2005 12:00:00 by admin");
            metadataLabel.setFont(new java.awt.Font("Dialog", 2, 12));
        }

        log.debug("getMetadataLabel() - end");
        return metadataLabel;
    }

    private JLabel getGroupLabel() {
        if (groupLabel == null) {
            groupLabel = new JLabel();
            groupLabel.setText("Group");
        }
        return groupLabel;
    }

    private JTextField getGroupNameTextField() {
        if (groupNameTextField == null) {
            groupNameTextField = new JTextField();
            Group groupName = pipelineDef.getGroup();
            if (groupName != null) {
                groupNameTextField.setText(groupName.toString());
            } else {
                groupNameTextField.setText("<default>");
            }
            groupNameTextField.setEditable(false);
        }
        return groupNameTextField;
    }
}

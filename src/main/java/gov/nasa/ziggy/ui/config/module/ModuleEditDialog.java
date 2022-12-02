package gov.nasa.ziggy.ui.config.module;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.ui.common.ClasspathUtils;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.PipelineModuleDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ModuleEditDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(ModuleEditDialog.class);

    private final PipelineModuleDefinition module;
    private JTextField minMemoryTextField;
    private JLabel minMemoryLabel;
    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JLabel nameLanel;
    private JTextField nameText;
    private JTextArea descTextArea;
    private JLabel exeNameLabel;
    private JComboBox<ClassWrapper<PipelineModule>> implementingClassComboBox;
    private JScrollPane descScrollPane;
    private JLabel versionLabel;
    private JTextField exeTimeoutTextField;
    private JLabel exeTimeoutLabel;
    private JTextField exeNameTextField;
    private JLabel descLabel;
    private JButton cancelButton;
    private JButton saveButton;
    private boolean cancelled = false;

    private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud;

    public ModuleEditDialog(JFrame frame, PipelineModuleDefinition module) {
        super(frame, "Edit Module " + module.getName(), true);
        this.module = module;
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();
        initGUI();
    }

    private void saveButtonActionPerformed(ActionEvent evt) {
        log.debug("saveButtonActionPerformed(ActionEvent) - start");

        log.debug("saveButton.actionPerformed, event=" + evt);

        /*
         * TODO: validate that the selected param sets are compatible with the selected
         * implementingClass
         */

        try {
            PipelineModuleDefinition newModule;

            if (module.isLocked()) {
                // module definition is locked, so make a new version
                newModule = module.newVersion();
            } else {
                // just update the existing instance
                newModule = module;
            }

            newModule.setDescription(descTextArea.getText());

            @SuppressWarnings("unchecked")
            ClassWrapper<PipelineModule> selectedImplementingClass = (ClassWrapper<PipelineModule>) implementingClassComboBox
                .getSelectedItem();

            newModule.setPipelineModuleClass(selectedImplementingClass);
            newModule.setExeTimeoutSecs(string2int(exeTimeoutTextField.getText(), 0));
            newModule.setMinMemoryMegaBytes(string2int(minMemoryTextField.getText(), 0));

            pipelineModuleDefinitionCrud.save(newModule);

            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("saveButtonActionPerformed(ActionEvent) - end");
    }

    /**
     * Convert the specified String to an int and return the defaultValue if the conversion fails
     *
     * @param s
     * @param defaultValue
     * @return
     */
    private int string2int(String s, int defaultValue) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        cancelled = true;

        setVisible(false);
    }

    private JComboBox<ClassWrapper<PipelineModule>> getImplementingClassComboBox() {
        if (implementingClassComboBox == null) {
            DefaultComboBoxModel<ClassWrapper<PipelineModule>> implementingClassComboBoxModel = new DefaultComboBoxModel<>();

            try {
                ClasspathUtils classpathUtils = new ClasspathUtils();
                Set<Class<? extends PipelineModule>> detectedClasses = classpathUtils
                    .scanFully(PipelineModule.class);
                List<ClassWrapper<PipelineModule>> wrapperList = new LinkedList<>();

                for (Class<? extends PipelineModule> clazz : detectedClasses) {
                    try {
                        ClassWrapper<PipelineModule> wrapper = new ClassWrapper<>(clazz);

                        wrapperList.add(wrapper);
                    } catch (Exception ignore) {
                    }
                }

                Collections.sort(wrapperList);

                int selectedIndex = -1;
                int index = 0;

                for (ClassWrapper<PipelineModule> classWrapper : wrapperList) {
                    implementingClassComboBoxModel.addElement(classWrapper);

                    ClassWrapper<PipelineModule> implementingClass = module
                        .getPipelineModuleClass();
                    if (implementingClass != null
                        && classWrapper.getClazz().equals(implementingClass.getClazz())) {
                        selectedIndex = index;
                    }
                    index++;
                }

                implementingClassComboBox = new JComboBox<>();
                implementingClassComboBox.setModel(implementingClassComboBoxModel);
                implementingClassComboBox
                    .addActionListener(evt -> implementingClassComboBoxActionPerformed());

                if (selectedIndex != -1) {
                    implementingClassComboBox.setSelectedIndex(selectedIndex);
                }
            } catch (Exception e) {
                MessageUtil.showError(this, e);
            }
        }
        return implementingClassComboBox;
    }

    private void implementingClassComboBoxActionPerformed() {
        /*
         * TODO check that the Parameters and Parameters are compatible with the selected
         * PipelineModule
         */
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            setTitle("Edit Module");
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            this.setSize(461, 389);
        } catch (Exception e) {
            log.error("initGUI()", e);

            e.printStackTrace();
        }

        log.debug("initGUI() - end");
    }

    private JPanel getDataPanel() {
        log.debug("getDataPanel() - start");

        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.rowHeights = new int[] {};
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getNameLanel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getNameText(),
                new GridBagConstraints(1, 0, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getDescLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.FIRST_LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getVersionLabel(),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getDescScrollPane(), new GridBagConstraints(1, 1, 4, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getImplementingClassComboBox(),
                new GridBagConstraints(1, 2, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getExeNameLabel(),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getExeTimeoutLabel(),
                new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getExeTimeoutTextField(),
                new GridBagConstraints(1, 4, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getMinMemoryLabel(),
                new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            dataPanel.add(getMinMemoryTextField(),
                new GridBagConstraints(1, 5, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        }

        log.debug("getDataPanel() - end");
        return dataPanel;
    }

    /**
     * Auto-generated method for setting the popup menu for a component
     */
    // private void setComponentPopupMenu(final java.awt.Component parent, final
    // javax.swing.JPopupMenu menu) {
    // log.debug("setComponentPopupMenu(java.awt.Component,
    // javax.swing.JPopupMenu) - start");
    //
    // parent.addMouseListener(new java.awt.event.MouseAdapter() {
    // public void mousePressed(java.awt.event.MouseEvent e) {
    // log.debug("mousePressed(java.awt.event.MouseEvent) - start");
    //
    // if (e.isPopupTrigger()){
    // popupRow = paramSetsTable.rowAtPoint(e.getPoint());
    // menu.show(parent, e.getX(), e.getY());
    // }
    // log.debug("mousePressed(java.awt.event.MouseEvent) - end");
    // }
    //
    // public void mouseReleased(java.awt.event.MouseEvent e) {
    // log.debug("mouseReleased(java.awt.event.MouseEvent) - start");
    //
    // if (e.isPopupTrigger()){
    // popupRow = paramSetsTable.rowAtPoint(e.getPoint());
    // menu.show(parent, e.getX(), e.getY());
    // }
    // log.debug("mouseReleased(java.awt.event.MouseEvent) - end");
    // }
    // });
    //
    // log.debug("setComponentPopupMenu(java.awt.Component,
    // javax.swing.JPopupMenu) - end");
    // }
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

                saveButtonActionPerformed(evt);

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

                cancelButtonActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getCancelButton() - end");
        return cancelButton;
    }

    private JLabel getNameLanel() {
        log.debug("getNameLanel() - start");

        if (nameLanel == null) {
            nameLanel = new JLabel();
            nameLanel.setText("Name");
        }

        log.debug("getNameLanel() - end");
        return nameLanel;
    }

    private JTextField getNameText() {
        log.debug("getNameText() - start");

        if (nameText == null) {
            nameText = new JTextField();
            nameText.setText(module.getName().getName());
            nameText.setEditable(false);
        }

        log.debug("getNameText() - end");
        return nameText;
    }

    private JLabel getDescLabel() {
        log.debug("getDescLabel() - start");

        if (descLabel == null) {
            descLabel = new JLabel();
            descLabel.setText("Description");
        }

        log.debug("getDescLabel() - end");
        return descLabel;
    }

    private JTextArea getDescTextArea() {
        log.debug("getDescTextArea() - start");

        if (descTextArea == null) {
            descTextArea = new JTextArea();
            descTextArea.setRows(4);
            descTextArea.setLineWrap(true);
            descTextArea.setWrapStyleWord(true);
            descTextArea.setText(module.getDescription());
        }

        log.debug("getDescTextArea() - end");
        return descTextArea;
    }

    private JLabel getVersionLabel() {
        log.debug("getVersionLabel() - start");

        if (versionLabel == null) {
            versionLabel = new JLabel();
            versionLabel.setText("Implementing class");
        }

        log.debug("getVersionLabel() - end");
        return versionLabel;
    }

    private JScrollPane getDescScrollPane() {
        log.debug("getDescScrollPane() - start");

        if (descScrollPane == null) {
            descScrollPane = new JScrollPane();
            descScrollPane.setViewportView(getDescTextArea());
        }

        log.debug("getDescScrollPane() - end");
        return descScrollPane;
    }

    private JLabel getExeNameLabel() {
        if (exeNameLabel == null) {
            exeNameLabel = new JLabel();
            exeNameLabel.setText("Executable name");
        }
        return exeNameLabel;
    }

    private JLabel getExeTimeoutLabel() {
        if (exeTimeoutLabel == null) {
            exeTimeoutLabel = new JLabel();
            exeTimeoutLabel.setText("Executable timeout (secs)");
        }
        return exeTimeoutLabel;
    }

    private JTextField getExeTimeoutTextField() {
        if (exeTimeoutTextField == null) {
            exeTimeoutTextField = new JTextField("" + module.getExeTimeoutSecs());
        }
        return exeTimeoutTextField;
    }

    private JLabel getMinMemoryLabel() {
        if (minMemoryLabel == null) {
            minMemoryLabel = new JLabel();
            minMemoryLabel.setText("Minimum Memory (mb)");
        }
        return minMemoryLabel;
    }

    private JTextField getMinMemoryTextField() {
        if (minMemoryTextField == null) {
            minMemoryTextField = new JTextField("" + module.getMinMemoryMegaBytes());
        }
        return minMemoryTextField;
    }

    public boolean isCancelled() {
        return cancelled;
    }
}

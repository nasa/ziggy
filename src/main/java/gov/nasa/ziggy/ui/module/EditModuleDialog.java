package gov.nasa.ziggy.ui.module;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.GroupLayout;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.ui.util.ClasspathUtils;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.proxy.PipelineModuleDefinitionCrudProxy;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class EditModuleDialog extends javax.swing.JDialog {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(EditModuleDialog.class);
    private static final long serialVersionUID = 20230824L;

    private final PipelineModuleDefinition module;
    private final PipelineModuleDefinitionCrudProxy pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrudProxy();

    private JTextArea descText;
    private JComboBox<ClassWrapper<PipelineModule>> implementingClassComboBox;
    private JTextField exeTimeoutText;
    private JTextField minMemoryText;

    private boolean cancelled = false;

    public EditModuleDialog(Window owner, PipelineModuleDefinition module) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.module = module;

        buildComponent();
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
        descText = new JTextArea();
        descText.setRows(4);
        descText.setLineWrap(true);
        descText.setWrapStyleWord(true);
        descText.setText(module.getDescription());
        JScrollPane descScrollPane = new JScrollPane(descText);

        JLabel implementingClass = boldLabel("Implementing class");
        implementingClassComboBox = createImplementingClassComboBox();

        JLabel exeTimeout = boldLabel("Executable timeout (seconds)");
        exeTimeoutText = new JTextField(Integer.toString(module.getExeTimeoutSecs()));
        exeTimeoutText.setColumns(15);

        JLabel minMemory = boldLabel("Minimum memory (MB)");
        minMemoryText = new JTextField(Integer.toString(module.getMinMemoryMegaBytes()));
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
            .addComponent(implementingClassComboBox, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
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
            .addComponent(implementingClassComboBox, GroupLayout.PREFERRED_SIZE,
                GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
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

    private JComboBox<ClassWrapper<PipelineModule>> createImplementingClassComboBox() {

        DefaultComboBoxModel<ClassWrapper<PipelineModule>> implementingClassComboBoxModel = new DefaultComboBoxModel<>();
        JComboBox<ClassWrapper<PipelineModule>> implementingClassComboBox = new JComboBox<>(
            implementingClassComboBoxModel);
        implementingClassComboBox.addActionListener(this::updateImplementingClass);

        try {
            Set<Class<? extends PipelineModule>> detectedClasses = ClasspathUtils
                .scanFully(PipelineModule.class);
            List<ClassWrapper<PipelineModule>> wrapperList = new LinkedList<>();

            for (Class<? extends PipelineModule> clazz : detectedClasses) {
                try {
                    wrapperList.add(new ClassWrapper<>(clazz));
                } catch (Exception ignore) {
                }
            }

            Collections.sort(wrapperList);

            int selectedIndex = -1;
            int index = 0;

            for (ClassWrapper<PipelineModule> classWrapper : wrapperList) {
                implementingClassComboBoxModel.addElement(classWrapper);

                ClassWrapper<PipelineModule> implementingClass = module.getPipelineModuleClass();
                if (implementingClass != null
                    && classWrapper.getClazz().equals(implementingClass.getClazz())) {
                    selectedIndex = index;
                }
                index++;
            }

            if (selectedIndex != -1) {
                implementingClassComboBox.setSelectedIndex(selectedIndex);
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
        return implementingClassComboBox;
    }

    private void updateImplementingClass(ActionEvent evt) {
        // TODO Validate that parameters are compatible with the selected PipelineModule (ZIGGY-224)
    }

    private void save(ActionEvent evt) {
        // TODO Validate that the parameters are compatible with the selected implementingClass
        // (ZIGGY-224)

        try {
            module.setDescription(descText.getText());

            @SuppressWarnings("unchecked")
            ClassWrapper<PipelineModule> selectedImplementingClass = (ClassWrapper<PipelineModule>) implementingClassComboBox
                .getSelectedItem();

            module.setPipelineModuleClass(selectedImplementingClass);
            module.setExeTimeoutSecs(toInt(exeTimeoutText.getText(), 0));
            module.setMinMemoryMegaBytes(toInt(minMemoryText.getText(), 0));

            pipelineModuleDefinitionCrud.createOrUpdate(module);

            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
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

    public boolean isCancelled() {
        return cancelled;
    }
}

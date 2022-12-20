package gov.nasa.ziggy.ui.config.parameters;

import java.awt.BorderLayout;

import javax.swing.JList;
import javax.swing.JScrollPane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.ui.common.MessageUtil;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterClassSelectorPanel extends javax.swing.JPanel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParameterClassSelectorPanel.class);

    private JScrollPane paramClassScrollPane;
    private JList<ClassWrapper<Parameters>> paramClassList;

    private ParameterClassListModel paramClassListModel;

    public ParameterClassSelectorPanel() {
        initGUI();
    }

    public ClassWrapper<Parameters> getSelectedElement() {
        int selectedIndex = paramClassList.getSelectedIndex();
        if (selectedIndex != -1) {
            return paramClassListModel.getElementAt(selectedIndex);
        }
        return null;
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            this.add(getParamClassScrollPane(), BorderLayout.CENTER);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private JScrollPane getParamClassScrollPane() throws Exception {
        if (paramClassScrollPane == null) {
            paramClassScrollPane = new JScrollPane();
            paramClassScrollPane.setViewportView(getParamClassList());
        }
        return paramClassScrollPane;
    }

    private JList<ClassWrapper<Parameters>> getParamClassList() throws Exception {
        if (paramClassList == null) {
            paramClassListModel = new AllParameterClassListModel();
            paramClassList = new JList<>();
            paramClassList.setModel(paramClassListModel);
        }
        return paramClassList;
    }
}

package gov.nasa.ziggy.ui.parameters;

import java.awt.BorderLayout;

import javax.swing.JList;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.ui.util.MessageUtil;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterClassSelectorPanel extends javax.swing.JPanel {
    private JList<ClassWrapper<Parameters>> paramClassList;
    private ParameterClassListModel paramClassListModel;

    public ParameterClassSelectorPanel() {
        buildComponent();
    }

    private void buildComponent() {
        try {
            paramClassListModel = new AllParameterClassListModel();
            paramClassList = new JList<>(paramClassListModel);
            setLayout(new BorderLayout());
            add(new JScrollPane(paramClassList), BorderLayout.CENTER);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    public ClassWrapper<Parameters> getSelectedElement() {
        int selectedIndex = paramClassList.getSelectedIndex();
        if (selectedIndex != -1) {
            return paramClassListModel.getElementAt(selectedIndex);
        }
        return null;
    }

    private static class AllParameterClassListModel extends ParameterClassListModel {
        public AllParameterClassListModel() throws Exception {
            super(ParametersClassCache.getCache());
        }
    }
}

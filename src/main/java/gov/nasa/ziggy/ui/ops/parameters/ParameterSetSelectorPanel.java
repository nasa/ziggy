package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JList;
import javax.swing.JScrollPane;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.ui.config.parameters.ParameterSetListModel;

/**
 * Select a {@link ParameterSetName} from a list or create a new one.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetSelectorPanel extends javax.swing.JPanel {
    private JList<ParameterSet> paramSetList;
    private JScrollPane scrollPane;
    private ParameterSetListModel paramSetListModel;

    private final Class<? extends Parameters> filterClass;

    public ParameterSetSelectorPanel() {
        filterClass = null;
        initGUI();
    }

    public ParameterSetSelectorPanel(Class<? extends Parameters> filterClass) {
        this.filterClass = filterClass;
        initGUI();
    }

    public ParameterSet getSelected() {
        int selectedIndex = paramSetList.getSelectedIndex();

        if (selectedIndex != -1) {
            return paramSetListModel.getElementAt(selectedIndex);
        }
        return null;
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(400, 300));
            {
                scrollPane = new JScrollPane();
                this.add(scrollPane, BorderLayout.CENTER);
                {
                    paramSetListModel = new ParameterSetListModel(filterClass);
                    paramSetList = new JList<>();
                    scrollPane.setViewportView(paramSetList);
                    paramSetList.setModel(paramSetListModel);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

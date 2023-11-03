package gov.nasa.ziggy.ui.pipeline;

import java.awt.BorderLayout;

import com.l2fprod.common.propertysheet.PropertySheet;
import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.ui.util.PropertySheetHelper;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditParametersPanel extends javax.swing.JPanel {
    private PropertySheetPanel propertySheetPanel;
    private ParametersInterface parameters;

    public EditParametersPanel(ParametersInterface parameters) {
        this.parameters = parameters;
        buildComponent();
    }

    private void buildComponent() {
        propertySheetPanel = new PropertySheetPanel();
        propertySheetPanel.setMode(PropertySheet.VIEW_AS_CATEGORIES);
        propertySheetPanel.setDescriptionVisible(true);
        propertySheetPanel.setSortingCategories(true);
        // propertySheetPanel.setSortingProperties(true);
        propertySheetPanel.getTable().setWantsExtraIndent(true);

        if (parameters != null) {
            try {
                PropertySheetHelper.populatePropertySheet(parameters, propertySheetPanel);
            } catch (Exception e) {
                throw new PipelineException("Failed to populate property sheet from parameters", e);
            }
        }

        setLayout(new BorderLayout());
        add(propertySheetPanel);
    }

    public ParametersInterface getParameters() throws PipelineException {
        propertySheetPanel.writeToObject(parameters);
        return parameters;
    }

    public void makeReadOnly() {
        propertySheetPanel.getTable().setEnabled(false);
    }
}

package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.BorderFactory;

import com.l2fprod.common.propertysheet.PropertySheet;
import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.ui.common.PropertySheetHelper;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditParametersPanel extends javax.swing.JPanel {
    private PropertySheetPanel propertySheetPanel = null;
    private Parameters parameters = null;

    public EditParametersPanel(Parameters parameters) {
        this.parameters = parameters;

        initGUI();
    }

    public EditParametersPanel() {
        initGUI();
    }

    public Parameters getParameters() throws PipelineException {
        propertySheetPanel.writeToObject(parameters);
        return parameters;
    }

    public void makeReadOnly() {
        propertySheetPanel.getTable().setEnabled(false);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(400, 300));
            setBorder(BorderFactory.createTitledBorder("Edit Parameters"));
            this.add(getPropertySheetPanel(), BorderLayout.CENTER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private PropertySheetPanel getPropertySheetPanel() {
        if (propertySheetPanel == null) {
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
                    throw new PipelineException("Failed to introspect Parameters bean", e);
                }
            }
        }
        return propertySheetPanel;
    }
}

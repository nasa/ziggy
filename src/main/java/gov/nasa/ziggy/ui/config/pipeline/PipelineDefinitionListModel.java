package gov.nasa.ziggy.ui.config.pipeline;

import java.util.List;

import javax.swing.ComboBoxModel;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.models.AbstractDatabaseListModel;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineDefinitionListModel extends AbstractDatabaseListModel<PipelineDefinition>
    implements ComboBoxModel<PipelineDefinition> {
    private List<PipelineDefinition> pipelineDefinitions = null;
    private PipelineDefinition selectedPipelineDefinition = null;
    PipelineDefinitionCrudProxy pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();

    public PipelineDefinitionListModel() {
    }

    @Override
    public void loadFromDatabase() {
        if (pipelineDefinitions != null) {
            pipelineDefinitionCrud.evictAll(pipelineDefinitions);
        }

        pipelineDefinitions = pipelineDefinitionCrud.retrieveLatestVersions();

        if (pipelineDefinitions.size() > 0) {
            selectedPipelineDefinition = pipelineDefinitions.get(0);
        }
    }

    @Override
    public PipelineDefinition getElementAt(int index) {
        validityCheck();
        return pipelineDefinitions.get(index);
    }

    @Override
    public int getSize() {
        validityCheck();
        return pipelineDefinitions.size();
    }

    @Override
    public Object getSelectedItem() {
        validityCheck();
        return selectedPipelineDefinition;
    }

    @Override
    public void setSelectedItem(Object anItem) {
        validityCheck();
        selectedPipelineDefinition = (PipelineDefinition) anItem;
    }
}

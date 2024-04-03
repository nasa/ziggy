package gov.nasa.ziggy.pipeline.definition;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Stores processing options for a given pipeline.
 * <p>
 * The ProcessingMode enumeration specifies whether to process all data, including data that has
 * already been processed, or to process only new data that has never been processed before.
 *
 * @author PT
 */
@Entity
@Table(name = "ziggy_PipelineDefinition_processingOptions")
public class PipelineDefinitionProcessingOptions {

    public enum ProcessingMode {
        PROCESS_NEW("new"), PROCESS_ALL("all");

        private String displayString;

        ProcessingMode(String displayString) {
            this.displayString = displayString;
        }

        @Override
        public String toString() {
            return displayString;
        }
    }

    @Id
    private String pipelineName;

    @Enumerated(EnumType.STRING)
    @Column(name = "processingMode")
    private ProcessingMode processingMode = ProcessingMode.PROCESS_ALL;

    public PipelineDefinitionProcessingOptions() {
    }

    public PipelineDefinitionProcessingOptions(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    public void setProcessingMode(ProcessingMode processingMode) {
        this.processingMode = processingMode;
    }
}

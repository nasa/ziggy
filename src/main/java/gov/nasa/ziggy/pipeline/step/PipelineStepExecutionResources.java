package gov.nasa.ziggy.pipeline.step;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

/**
 * Execution resources for {@link PipelineStep} instances. The execution resources table is not
 * linked to the step by a foreign key constraint. Rather, the name of the step is stored along with
 * its parameters. This ensures that a single instance of {@link PipelineStepExecutionResources} is
 * associated with all versions of the {@link PipelineStep} in the database, and, conversely,
 * changing the resource parameters does not cause the pipeline to update to a new version.
 *
 * @author PT
 */
@Entity
@Table(name = "ziggy_PipelineStepExecutionResources")
public class PipelineStepExecutionResources {

    @Transient
    public static final int DEFAULT_TIMEOUT_SECONDS = 60 * 60 * 50; // 50 hours.

    @Transient
    public static final int DEFAULT_MEMORY_MEGABYTES = 0; // 0 means memory usage not constrained.

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineStepxecutionResources_generator")
    @SequenceGenerator(name = "ziggy_PipelineStepExecutionResources_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineStepExecutionResources_sequence", allocationSize = 1)
    private Long id;

    private String pipelineStepName;
    private int exeTimeoutSeconds;
    private int minMemoryMegabytes;

    public String getPipelineStepName() {
        return pipelineStepName;
    }

    public void setPipelineStepName(String pipelineStepName) {
        this.pipelineStepName = pipelineStepName;
    }

    public int getExeTimeoutSeconds() {
        return exeTimeoutSeconds;
    }

    public void setExeTimeoutSeconds(int exeTimeoutSeconds) {
        this.exeTimeoutSeconds = exeTimeoutSeconds;
    }

    public int getMinMemoryMegabytes() {
        return minMemoryMegabytes;
    }

    public void setMinMemoryMegabytes(int minMemoryMegabytes) {
        this.minMemoryMegabytes = minMemoryMegabytes;
    }
}

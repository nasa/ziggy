package gov.nasa.ziggy.pipeline.definition;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

/**
 * Execution resources for {@link PipelineModuleDefinition} instances. The execution resources table
 * is not linked to the module definition by a foreign key constraint. Rather, the name of the
 * module is stored along with its parameters. This ensures that a single instance of
 * {@link PipelineModuleExecutionResources} is associated with all versions of the
 * {@link PipelineModuleDefinition} in the database, and, conversely, changing the resource
 * parameters does not cause the module definition to update to a new version.
 *
 * @author PT
 */
@Entity
@Table(name = "ziggy_PipelineModuleExecutionResources")
public class PipelineModuleExecutionResources {

    @Transient
    public static final int DEFAULT_TIMEOUT_SECONDS = 60 * 60 * 50; // 50 hours.

    @Transient
    public static final int DEFAULT_MEMORY_MEGABYTES = 0; // 0 means memory usage not constrained.

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineModuleExecutionResources_generator")
    @SequenceGenerator(name = "ziggy_PipelineModuleExecutionResources_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineModuleExecutionResources_sequence", allocationSize = 1)
    private Long id;

    private String pipelineModuleName;
    private int exeTimeoutSeconds;
    private int minMemoryMegabytes;

    public String getPipelineModuleName() {
        return pipelineModuleName;
    }

    public void setPipelineModuleName(String pipelineModuleName) {
        this.pipelineModuleName = pipelineModuleName;
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

package gov.nasa.ziggy.pipeline.definition;

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Represents a single pipeline unit of work associated with a {@link PipelineInstance}, a
 * {@link PipelineDefinitionNode} (which is associated with a {@link PipelineModuleDefinition}), and
 * a {@link UnitOfWorkGenerator} that represents the unit of work.
 * <p>
 * This class is immutable and is a suitable handle for code that operates on a pipeline task.
 * Mutable fields associated with a {@code PipelineTask} are found in {@code PipelineTaskData}.
 * <p>
 * Note that the {@link #equals(Object)} and {@link #hashCode()} methods are written in terms of
 * just the {@code id} field so this object should not be used in sets and maps until it has been
 * stored in the database
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@Entity
@Table(name = "ziggy_PipelineTask")
public class PipelineTask implements Serializable, Comparable<PipelineTask> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineTask.class);

    private static final long serialVersionUID = 20240909L;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_PipelineTask_generator")
    @SequenceGenerator(name = "ziggy_PipelineTask_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineTask_sequence", allocationSize = 1)
    private Long id;

    private long pipelineInstanceId;

    private String moduleName;

    private String executableName;

    /** Timestamp this task was created (either by launcher or transition logic) */
    private Date created = new Date();

    @Embedded
    private UnitOfWork unitOfWork;

    /**
     * Required by Hibernate
     */
    public PipelineTask() {
    }

    public PipelineTask(PipelineInstance pipelineInstance,
        PipelineInstanceNode pipelineInstanceNode, UnitOfWork unitOfWork) {

        // The pipelineInstanceNode can be null in tests.
        if (pipelineInstanceNode != null) {
            moduleName = pipelineInstanceNode.getModuleName();
            executableName = pipelineInstanceNode.getExecutableName();
        }

        // The pipelineInstance can be null in tests.
        if (pipelineInstance != null && pipelineInstance.getId() != null) {
            pipelineInstanceId = pipelineInstance.getId();
        }

        this.unitOfWork = unitOfWork;
    }

    public String taskBaseName() {
        return getPipelineInstanceId() + "-" + getId() + "-" + getModuleName();
    }

    public Long getId() {
        return id;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getExecutableName() {
        return executableName;
    }

    public Date getCreated() {
        return created;
    }

    public UnitOfWork getUnitOfWork() {
        return unitOfWork;
    }

    @Override
    public int compareTo(PipelineTask o) {
        return (int) (getId() - o.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineTask other = (PipelineTask) obj;
        return Objects.equals(id, other.id);
    }

    public String toFullString() {
        return "IID=" + getPipelineInstanceId() + ", TID=" + getId() + ", M=" + getModuleName()
            + ", UOW=" + getUnitOfWork().briefState();
    }

    @Override
    public String toString() {
        return getId().toString();
    }

    public static final class TaskBaseNameMatcher {
        public static final int INSTANCE_ID_GROUP = 1;
        public static final int TASK_ID_GROUP = 2;
        public static final int MODULE_NAME_GROUP = 3;
        public static final int JOB_INDEX_GROUP = 5;
        public static final String regex = "(\\d+)-(\\d+)-([^\\s\\.]+){1}?(\\.(\\d+))?";
        public static final Pattern pattern = Pattern.compile(regex);

        private String baseName;
        private boolean matches;
        private long instanceId;
        private long taskId;
        private String moduleName;
        private boolean hasJobIndex;
        private int jobIndex;

        public TaskBaseNameMatcher(String baseName) {
            this.baseName = baseName;
            Matcher matcher = pattern.matcher(baseName);
            matches = matcher.matches();
            if (matches) {
                instanceId = Long.parseLong(matcher.group(INSTANCE_ID_GROUP));
                taskId = Long.parseLong(matcher.group(TASK_ID_GROUP));
                moduleName = matcher.group(MODULE_NAME_GROUP);
                hasJobIndex = matcher.group(JOB_INDEX_GROUP) != null;
                if (hasJobIndex) {
                    jobIndex = Integer.parseInt(matcher.group(JOB_INDEX_GROUP));
                }
            }
        }

        public boolean matches() {
            return matches;
        }

        public long instanceId() {
            checkMatch();
            return instanceId;
        }

        public long taskId() {
            checkMatch();
            return taskId;
        }

        public String moduleName() {
            checkMatch();
            return moduleName;
        }

        public boolean hasJobIndex() {
            return hasJobIndex;
        }

        public int jobIndex() {
            checkMatch();
            checkHasJobIndex();
            return jobIndex;
        }

        private void checkMatch() {
            checkState(matches, "String " + baseName + " does not match task base name pattern");
        }

        private void checkHasJobIndex() {
            checkState(matches, "String " + baseName + " does not have a job index");
        }
    }
}

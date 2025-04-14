package gov.nasa.ziggy.pipeline.step.remote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionElement;
import gov.nasa.ziggy.pipeline.step.remote.batch.SupportedBatchSystem;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.AttributeOverrides;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlTransient;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * Models a remote environment. A remote environment is an external system that performs batch
 * processing of pipeline tasks. Class instances specify the name of the remote system, the batch
 * system that manages jobs, the batch queues and batch architectures available on the remote
 * system.
 *
 * @author PT
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_remoteEnvironment")
public class RemoteEnvironment implements PipelineDefinitionElement {

    @Transient
    @XmlTransient
    public static final String ANY_ARCHITECTURE = "Any";

    @Transient
    @XmlTransient
    public static final String ANY_QUEUE = "Any";
    /**
     * Static instances of any {@link QueueTimeMetrics} class that have been used during execution.
     */
    @Transient
    @XmlTransient
    private static final Map<Class<? extends QueueTimeMetrics>, QueueTimeMetrics> queueTimeMetricsByClass = new ConcurrentHashMap<>();

    @XmlAttribute(required = true)
    @Id
    private String name;

    @XmlAttribute(required = true)
    private String description;

    @XmlTransient
    @Enumerated(EnumType.STRING)
    private SupportedBatchSystem batchSystem;

    @XmlTransient
    @OneToMany(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_remoteEnvironment_architectures")
    @Cascade({ CascadeType.PERSIST, CascadeType.MERGE, CascadeType.DETACH })
    private List<Architecture> architectures;

    @XmlTransient
    @OneToMany(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_remoteEnvironment_batchQueues")
    @Cascade({ CascadeType.PERSIST, CascadeType.MERGE, CascadeType.DETACH })
    private List<BatchQueue> queues;

    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "queueTimeMetricsClass")) })
    private ClassWrapper<QueueTimeMetrics> queueTimeMetricsClass;

    /** Cost unit (SBUs, $, etc.) */
    @XmlAttribute(required = true)
    private String costUnit;

    @Transient
    @XmlAttribute(required = true, name = "batchSystem")
    private String batchSystemName;

    // The following construction makes it possible to put the architectures and batch queues for a
    // remote environment in any order in the XML, but it also complicates the bookkeeping required
    // for those objects.
    @XmlElements({ @XmlElement(name = "architecture", type = Architecture.class),
        @XmlElement(name = "queue", type = BatchQueue.class) })
    @Transient
    private Set<Object> architecturesAndBatchQueues = new HashSet<>();

    public void populateDatabaseFields() {
        architectures = CollectionFilters.filterToList(architecturesAndBatchQueues,
            Architecture.class);
        queues = CollectionFilters.filterToList(architecturesAndBatchQueues, BatchQueue.class);
        batchSystem = SupportedBatchSystem.valueOf(batchSystemName.toUpperCase());
    }

    public void populateXmlFields() {
        batchSystemName = batchSystem.toString();
        architecturesAndBatchQueues.addAll(architectures);
        architecturesAndBatchQueues.addAll(queues);
    }

    public void updateFrom(RemoteEnvironment importedRemoteEnvironment,
        List<Architecture> updatedArchitectures, List<BatchQueue> updatedBatchQueues) {
        if (!name.equals(importedRemoteEnvironment.name)) {
            throw new IllegalArgumentException("Imported environment has different name");
        }
        description = importedRemoteEnvironment.description;
        batchSystem = importedRemoteEnvironment.batchSystem;
        queueTimeMetricsClass = importedRemoteEnvironment.queueTimeMetricsClass;
        costUnit = importedRemoteEnvironment.costUnit;
        architectures = updatedArchitectures;
        queues = updatedBatchQueues;
    }

    public List<String> architectureDescriptions() {
        List<String> architectureDescriptionsIncludingAny = new ArrayList<>();
        architectureDescriptionsIncludingAny.add(ANY_ARCHITECTURE);
        if (architectures == null) {
            return architectureDescriptionsIncludingAny;
        }
        List<String> architectureDescriptions = architectures.stream()
            .map(Architecture::getDescription)
            .sorted()
            .collect(Collectors.toList());
        architectureDescriptionsIncludingAny.addAll(architectureDescriptions);
        return architectureDescriptionsIncludingAny;
    }

    public Map<String, Architecture> architectureByDescription() {
        Map<String, Architecture> architectureByDescription = new HashMap<>();
        if (architectures == null) {
            return architectureByDescription;
        }
        for (Architecture architecture : architectures) {
            architectureByDescription.put(architecture.getDescription(), architecture);
        }
        return architectureByDescription;
    }

    public List<String> queueDescriptions() {
        List<String> queueDescriptionsIncludingAny = new ArrayList<>();
        queueDescriptionsIncludingAny.add(ANY_QUEUE);
        if (queues == null) {
            return queueDescriptionsIncludingAny;
        }
        List<String> queueDescriptions = queues.stream()
            .map(BatchQueue::getDescription)
            .sorted()
            .collect(Collectors.toList());
        queueDescriptionsIncludingAny.addAll(queueDescriptions);
        return queueDescriptionsIncludingAny;
    }

    public Map<String, BatchQueue> queueByDescription() {
        Map<String, BatchQueue> queueByDescription = new HashMap<>();
        if (queues == null) {
            return queueByDescription;
        }
        for (BatchQueue queue : queues) {
            queueByDescription.put(queue.getDescription(), queue);
        }
        return queueByDescription;
    }

    /**
     * Returns a static instance of the {@link QueueTimeMetrics} class for a given implementation
     * class. Use of a static instance allows the callers to use cached values rather than run the
     * metrics generation all over again every time it's called.
     */
    @SuppressWarnings("unchecked")
    public QueueTimeMetrics queueTimeMetricsInstance() {
        if (queueTimeMetricsClass == null) {
            return null;
        }
        if (!queueTimeMetricsByClass.containsKey(queueTimeMetricsClass.getClass())) {
            QueueTimeMetrics queueTimeMetrics = queueTimeMetricsClass.newInstance();
            queueTimeMetrics.setArchitectures(architectures);
            queueTimeMetricsByClass.put(
                (Class<? extends QueueTimeMetrics>) queueTimeMetricsClass.getClass(),
                queueTimeMetrics);
        }
        return queueTimeMetricsByClass.get(queueTimeMetricsClass.getClass());
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public SupportedBatchSystem getBatchSystem() {
        return batchSystem;
    }

    public List<Architecture> getArchitectures() {
        return architectures;
    }

    public List<BatchQueue> getQueues() {
        return queues;
    }

    public ClassWrapper<QueueTimeMetrics> getQueueTimeMetricsClass() {
        return queueTimeMetricsClass;
    }

    public String getCostUnit() {
        return costUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteEnvironment other = (RemoteEnvironment) obj;
        return Objects.equals(name, other.name);
    }
}

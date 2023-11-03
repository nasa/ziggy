package gov.nasa.ziggy.data.management;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Database table that tracks the producer task ID for each file in the datastore, and also the IDs
 * for each task that consumed the file (here "consumed" means "used as input in a process that ran
 * to completion"). This is used to support data accountability at the level of files and also
 * {@link PipelineTask} instances.
 * <p>
 * Note that while the {@link DatastoreProducerConsumer} primarily tracks mission data files, it is
 * also used to track the tasks that performed imports of instrument models. The model files do not
 * get their consumers tracked by the {@link DatastoreProducerConsumer}, instead there is a
 * {@link ModelRegistry} of the current versions of all models that is provided to a
 * {@link PipelineInstance} when the instance is created, and which can be exposed by the instance
 * report.
 *
 * @author PT
 */

@Entity
@Table(name = "ziggy_DatastoreProducerConsumer")
public class DatastoreProducerConsumer {

    public enum DataReceiptFileType {
        DATA, MODEL;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_DatastoreProducerConsumer_generator")
    @SequenceGenerator(name = "ziggy_DatastoreProducerConsumer_generator", initialValue = 1,
        sequenceName = "ziggy_DatastoreProducerConsumer_sequence", allocationSize = 1)
    private Long id;

    @Column(nullable = false, columnDefinition = "varchar(1000000)", unique = true)
    private String filename;

    private long producerId;

    @Column
    @Enumerated(EnumType.STRING)
    private DataReceiptFileType dataReceiptFileType;

    @ElementCollection
    @JoinTable(name = "ziggy_DatastoreProducerConsumer_consumers")
    private Set<Long> consumers = new TreeSet<>();

    // Needed by Hibernate.
    public DatastoreProducerConsumer() {
    }

    public DatastoreProducerConsumer(long producerId, String filename,
        DataReceiptFileType dataReceiptFileType) {
        checkNotNull(dataReceiptFileType, "dataReceiptFileType");
        checkNotNull(filename, "filename");
        this.dataReceiptFileType = dataReceiptFileType;
        this.filename = filename;
        this.producerId = producerId;
    }

    public DatastoreProducerConsumer(PipelineTask pipelineTask, Path datastoreFile,
        DataReceiptFileType dataReceiptFileType) {
        this(pipelineTask.getId(), datastoreFile.toString(), dataReceiptFileType);
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }

    public long getProducer() {
        return producerId;
    }

    public void setProducer(long producer) {
        producerId = producer;
    }

    public DataReceiptFileType getDataReceiptFileType() {
        return dataReceiptFileType;
    }

    public void setDataReceiptFileType(DataReceiptFileType dataReceiptFileType) {
        this.dataReceiptFileType = dataReceiptFileType;
    }

    public Set<Long> getConsumers() {
        return consumers;
    }

    // Use this method to get both the consumers that produced results and the
    // consumers that produced no results but did complete processing.
    public Set<Long> getAllConsumers() {
        return consumers.stream().map(Math::abs).collect(Collectors.toSet());
    }

    public void setConsumers(Set<Long> consumers) {
        this.consumers.addAll(consumers);
    }

    public void addConsumer(long consumer) {
        consumers.add(consumer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DatastoreProducerConsumer other = (DatastoreProducerConsumer) obj;
        if (!Objects.equals(filename, other.filename)) {
            return false;
        }
        return true;
    }
}

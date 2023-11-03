package gov.nasa.ziggy.data.management;

import java.nio.file.Path;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumer.DataReceiptFileType;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Provides tracking information for mission data files and instrument models that fail to import.
 * This can occur because the file fails a validation test, or because of a system problem that
 * prevents the movement of the file to the datastore.
 *
 * @author PT
 */
@Entity
@Table(name = "ziggy_FailedImport")
public class FailedImport {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_FailedImport_generator")
    @SequenceGenerator(name = "ziggy_FailedImport_generator", initialValue = 1,
        sequenceName = "ziggy_FailedImport_sequence", allocationSize = 1)
    private Long id;

    @Column
    private long dataReceiptTaskId;

    /**
     * Name of the file in datastore format. Note that there is no uniqueness constraint on
     * filenames in this table: you can screw up importing a file as many times as you like, and
     * every attempt will be preserved in the {@link FailedImport} database table's permanent
     * record.
     */
    @Column(nullable = false, columnDefinition = "varchar(1000000)", unique = false)
    private String filename;

    @Column
    @Enumerated(EnumType.STRING)
    private DataReceiptFileType dataReceiptFileType;

    // Needed by Hibernate.
    @SuppressWarnings("unused")
    private FailedImport() {
    }

    /**
     * Public constructor.
     *
     * @param task {@link PipelineTask} that attempted the import.
     * @param filename {@link Path} for the file in datastore format. Note that this path must be
     * relative to the datastore root.
     * @param dataReceiptFileType Type of file (data or model).
     */
    public FailedImport(PipelineTask task, Path filename, DataReceiptFileType dataReceiptFileType) {
        dataReceiptTaskId = task.getId();
        this.filename = filename.toString();
        this.dataReceiptFileType = dataReceiptFileType;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public long getDataReceiptTaskId() {
        return dataReceiptTaskId;
    }

    public void setDataReceiptTaskId(long dataReceiptTaskId) {
        this.dataReceiptTaskId = dataReceiptTaskId;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public DataReceiptFileType getDataReceiptFileType() {
        return dataReceiptFileType;
    }

    public void seDataReceiptFileType(DataReceiptFileType dataReceiptFileType) {
        this.dataReceiptFileType = dataReceiptFileType;
    }
}

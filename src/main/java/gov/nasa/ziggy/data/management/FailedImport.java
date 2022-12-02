package gov.nasa.ziggy.data.management;

import java.nio.file.Path;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumer.DataReceiptFileType;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Provides tracking information for mission data files and instrument models that fail to import.
 * This can occur because the file fails a validation test, or because of a system problem that
 * prevents the movement of the file to the datastore.
 *
 * @author PT
 */
@Entity
@Table(name = "PI_FAILED_IMPORT")
public class FailedImport {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_FAILED_IMPORT_SEQ",
        allocationSize = 1)
    private long id;

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

    public long getId() {
        return id;
    }

    public void setId(long id) {
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

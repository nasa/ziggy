package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import gov.nasa.ziggy.models.SemanticVersionNumber;
import gov.nasa.ziggy.util.Iso8601Formatter;

/**
 * Entity used to track revisions of models used in the system for data accountability purposes.
 * This metadata is updated by the model importers.
 * <p>
 * The metadata contains only the name and revision of the model (as Strings). It does not contain a
 * reference to the model itself, so there is no dependency on model entities or code.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_MODEL_METADATA")
public class ModelMetadata implements Comparable<ModelMetadata> {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_MODEL_METADATA_SEQ",
        allocationSize = 1)
    private long id;

    /** A String identifying the model type (like 'GEOMETRY' or 'SPACECRAFT_EPHEMERIS') */
    @ManyToOne(fetch = FetchType.EAGER)
    @XmlAttribute(required = true, name = "type")
    @XmlJavaTypeAdapter(ModelMetadata.ModelTypeAdapter.class)
    private ModelType modelType;

    /**
     * A description of the revision, usually provided by the operator when the update is imported.
     */
    @XmlAttribute(required = true, name = "description")
    private String modelDescription;

    /**
     * The file name of the model file once it's been stored in the datastore. This may be the
     * original file name or may include prepended version numbers and/or timestamps.
     */
    private String datastoreFileName;

    /**
     * The original file name. Used when the model is copied to the task directory, so that users
     * don't need to learn now Ziggy name mangling works.
     */
    private String originalFileName;

    /**
     * A String that uniquely identifies the revision. This must be a String that can be interpreted
     * as either an integer or a SemanticVersionNumber.
     */
    @XmlAttribute(required = true, name = "revision")
    private String modelRevision;

    /** The timestamp when the revision was imported */
    @XmlAttribute(required = true, name = "importTime")
    @XmlJavaTypeAdapter(ModelMetadata.DateAdapter.class)
    private Date importTime;

    /** The timestamp when this revision was locked (referenced by a PipelineInstance) */
    private Date lockTime;

    /**
     * Set to true when this revision is referenced by a {@link PipelineInstance}. Subsequent
     * updates result in a new row in this table
     */
    private boolean locked = false;

    /**
     * Task ID of the {@link PipelineTask} that imported the model.
     */
    private long dataReceiptTaskId;

    public ModelMetadata() {
    }

    /**
     * Constructor. Will throw an IllegalArgumentException if the version of this instance is &lt;=
     * the version in the current registry metadata.
     *
     * @param modelType
     * @param modelName
     * @param modelDescription
     * @param currentRegistryMetadata
     */
    public ModelMetadata(ModelType modelType, String modelName, String modelDescription,
        ModelMetadata currentRegistryMetadata) {

        String version = getNewVersion(modelType, modelName, currentRegistryMetadata);
        String datastoreName = datastoreName(modelType, modelName, version);
        importTime = currentDate();
        locked = false;
        modelRevision = version;
        datastoreFileName = datastoreName;
        originalFileName = modelName;
        this.modelDescription = modelDescription;
        this.modelType = modelType;
        if (currentRegistryMetadata != null && compareTo(currentRegistryMetadata) <= 0) {
            throw new IllegalArgumentException("New metadata of type " + modelType.getType()
                + " and version " + version + " cannot be imported because existing version "
                + currentRegistryMetadata.getModelRevision() + " has a higher version");
        }
    }

    /**
     * Copy constructor. Used for test purposes only, specifically to produce a
     * {@link ModelMetadata} instance from an instance of a subclass.
     */
    ModelMetadata(ModelMetadata original) {
        importTime = original.importTime;
        modelType = original.modelType;
        modelDescription = original.modelDescription;
        modelRevision = original.modelRevision;
        lockTime = original.lockTime;
        locked = original.locked;
        datastoreFileName = original.datastoreFileName;
        originalFileName = original.originalFileName;
    }

    /**
     * Returns the version string for the new instance. If the model's file name includes a version
     * number, this is returned. Otherwise, if there is a version in the model registry already,
     * then the new version will be 1 larger than the current one. If neither of the preceding is
     * true, the version will be "1".
     */
    private String getNewVersion(ModelType modelType, String modelName,
        ModelMetadata currentRegistryMetadata) {
        String version;
        // Do different things depending on whether the model type includes a version
        if (modelType.getVersionNumberGroup() > 0) {
            Matcher matcher = modelType.pattern().matcher(modelName);
            matcher.matches();
            version = matcher.group(modelType.getVersionNumberGroup());
        } else {
            if (currentRegistryMetadata != null) {
                int currentRegistryVersionInt = Integer
                    .parseInt(currentRegistryMetadata.getModelRevision());
                version = Integer.toString(++currentRegistryVersionInt);
            } else {
                version = "1";
            }
        }
        return version;
    }

    /**
     * Returns the name for this file in the datastore. If the file name contains a version number
     * and a timestamp, then the datastore file name is the existing file name. Otherwise, either
     * the current date, or the version number, or both, are prepended to the file name.
     */
    private String datastoreName(ModelType modelType, String modelName, String version) {

        StringBuilder stringBuilder = new StringBuilder();
        if (modelType.getTimestampGroup() <= 0) {
            stringBuilder.append(Iso8601Formatter.dateFormatter().format(currentDate()));
            stringBuilder.append(".");
        }
        if (modelType.getVersionNumberGroup() <= 0) {
            stringBuilder.append(String.format("%04d", Integer.parseInt(version)));
            stringBuilder.append("-");
        }
        stringBuilder.append(modelName);
        return stringBuilder.toString();
    }

    public void lock() {
        locked = true;
        lockTime = currentDate();
    }

    public ModelType getModelType() {
        return modelType;
    }

    public void setModelType(ModelType modelType) {
        this.modelType = modelType;
    }

    public String getModelDescription() {
        return modelDescription;
    }

    public void setModelDescription(String modelDescription) {
        this.modelDescription = modelDescription;
    }

    public String getModelRevision() {
        return modelRevision;
    }

    public void setModelRevision(String modelRevision) {
        this.modelRevision = modelRevision;
    }

    public Date getImportTime() {
        return importTime;
    }

    public void setImportTime(Date importTime) {
        this.importTime = importTime;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public long getId() {
        return id;
    }

    public Date getLockTime() {
        return lockTime;
    }

    public String getDatastoreFileName() {
        return datastoreFileName;
    }

    public void setDatastoreFileName(String modelFileName) {
        datastoreFileName = modelFileName;
    }

    public String getOriginalFileName() {
        return originalFileName;
    }

    public void setOriginalFileName(String modelFileName) {
        originalFileName = modelFileName;
    }

    public long getDataReceiptTaskId() {
        return dataReceiptTaskId;
    }

    public void setDataReceiptTaskId(long importTaskId) {
        dataReceiptTaskId = importTaskId;
    }

    @Override
    public int compareTo(ModelMetadata other) {
        if (!modelType.equals(other.modelType)) {
            throw new IllegalArgumentException("Comparison between types " + modelType.getType()
                + " and " + other.modelType.getType() + " is not defined");
        }
        int comparisonValue;
        if (modelType.isSemanticVersionNumber()) {
            comparisonValue = new SemanticVersionNumber(modelRevision)
                .compareTo(new SemanticVersionNumber(other.modelRevision));
        } else {
            comparisonValue = Integer.compare(Integer.valueOf(modelRevision),
                Integer.valueOf(other.modelRevision));
        }
        return comparisonValue;
    }

    Date currentDate() {
        return new Date();
    }

    private class DateAdapter extends XmlAdapter<XMLGregorianCalendar, Date> {

        @Override
        public Date unmarshal(XMLGregorianCalendar v) throws Exception {
            return v.toGregorianCalendar().getTime();
        }

        @Override
        public XMLGregorianCalendar marshal(Date v) throws Exception {
            GregorianCalendar gc = new GregorianCalendar();
            gc.setTime(v);
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);
        }

    }

    private class ModelTypeAdapter extends XmlAdapter<String, ModelType> {

        @Override
        public ModelType unmarshal(String v) throws Exception {
            ModelType modelType = new ModelType();
            modelType.setType(v);
            return modelType;
        }

        @Override
        public String marshal(ModelType v) throws Exception {
            return modelType.getType();
        }

    }
}

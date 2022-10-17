package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;

import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;

/**
 * Maintains a list of {@link ModelMetadata} objects for data accountability purposes.
 * {@link PipelineInstance} contains a reference to the specific version of the model registry that
 * was in force at the time the instance was launched.
 *
 * @author Todd Klaus
 */
@XmlRootElement(name = "modelRegistry")
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_MODEL_REGISTRY")
public class ModelRegistry implements HasXmlSchemaFilename {

    private static final String XML_SCHEMA_FILE_NAME = "model-registry.xsd";

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_MODREG_SEQ",
        allocationSize = 1)
    private long id;

    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(name = "PI_MODREG_MODEL")
    @Cascade({ CascadeType.SAVE_UPDATE })
    private Map<ModelType, ModelMetadata> models = new HashMap<>();

    @XmlElement(name = "modelMetadata")
    @Transient
    private List<ModelMetadata> modelMetadata = new ArrayList<>();

    private boolean locked = false;
    private Date lockTime;

    @Transient
    Map<String, ModelType> modelTypesMap = new HashMap<>();

    public ModelRegistry() {
    }

    /**
     * Copy constructor
     */
    public ModelRegistry(ModelRegistry other) {
        models = new HashMap<>(other.models);

        locked = false;
    }

    /**
     * Adds or replaces a model. If the registry is locked, an UnsupportedOperationException will
     * occur. If the new model has a revision number below the existing model (if any), an
     * IllegalArgumentException will occur.
     */
    public void updateModelMetadata(ModelMetadata modelMetadata) {
        if (locked) {
            throw new UnsupportedOperationException("Cannot upate models in a locked registry");
        }
        ModelType modelType = modelMetadata.getModelType();
        ModelMetadata existingModel = models.get(modelType);
        if (existingModel != null && modelMetadata.compareTo(existingModel) < 0) {
            throw new IllegalArgumentException(
                "New model version " + modelMetadata.getModelRevision()
                    + " cannot replace existing model version " + existingModel.getModelRevision());
        }
        models.put(modelType, modelMetadata);
    }

    public void populateXmlFields() {
        modelMetadata.clear();
        modelMetadata.addAll(models.values());
    }

    public void lock() {
        locked = true;
        lockTime = new Date();
    }

    public Date getLockTime() {
        return lockTime;
    }

    public boolean isLocked() {
        return locked;
    }

    public long getId() {
        return id;
    }

    public Map<ModelType, ModelMetadata> getModels() {
        return models;
    }

    private Map<String, ModelType> modelTypesMap() {
        if (modelTypesMap.isEmpty()) {
            for (ModelType modelType : models.keySet()) {
                modelTypesMap.put(modelType.getType(), modelType);
            }
        }
        return modelTypesMap;
    }

    /**
     * Convenience method to return {@link ModelMetadata} for the specified model type.
     *
     * @param modelType
     * @return
     */
    public ModelMetadata getMetadataForType(String modelType) {
        return models.get(modelTypesMap().get(modelType));
    }

    public ModelMetadata getMetadataForType(ModelType modelType) {
        return models.get(modelType);
    }

    public List<ModelMetadata> getModelMetadata() {
        return modelMetadata;
    }

    public void setModelMetadata(List<ModelMetadata> modelMetadata) {
        this.modelMetadata = modelMetadata;
    }

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }

}

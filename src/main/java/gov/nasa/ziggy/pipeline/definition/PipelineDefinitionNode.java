package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.xml.XmlReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.InputTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ModelTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.OutputTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ParameterSetReference;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;

/**
 * This class models a single node in a pipeline definition. Each node maps to a
 * {@link PipelineModuleDefinition} that specifies which algorithm is executed at this node. A node
 * is executed once for each unit of work. A node may have any number of nextNodes, which are
 * executed in parallel once this node completes.
 *
 * @author Todd Klaus
 */

@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_PipelineDefinitionNode")
public class PipelineDefinitionNode {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineDefinitionNode_generator")
    @SequenceGenerator(name = "ziggy_PipelineDefinitionNode_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineDefinitionNode_sequence", allocationSize = 1)
    private Long id;

    /**
     * Indicates the maximum number of worker processes that should be spun up for this node. If
     * left out of the node definition, the default maximum worker process count will be used.
     * <p>
     * This field is used for XML import only. The value is then stored in an instance of
     * {@link PipelineDefinitionNodeExecutionResources).
     * <p>
     * Optional XML attributes cannot be primitives.
     */
    @Transient
    @XmlAttribute(required = false, name = "maxWorkers")
    @Column(name = "maxWorkerCount", nullable = true)
    private Integer maxWorkerCount;

    /**
     * Indicates the maximum Java heap that should be allocated up for this node. If left out of the
     * node definition, the default maximum worker process count will be used.
     * <p>
     * This field is used for XML import only. The value is then stored in an instance of
     * {@link PipelineDefinitionNodeExecutionResources).
     * <p>
     * Optional XML attributes cannot be primitives.
     */
    @Transient
    @XmlAttribute(required = false, name = "heapSizeMb")
    @Column(name = "heapSizeMb", nullable = true)
    private Integer heapSizeMb;

    @XmlAttribute(required = true, name = "moduleName")
    private String moduleName;

    @OneToMany(fetch = FetchType.EAGER)
    @Cascade({ CascadeType.PERSIST, CascadeType.MERGE, CascadeType.DETACH })
    @JoinTable(name = "ziggy_PipelineDefinitionNode_nextNodes",
        joinColumns = @JoinColumn(name = "parentNodeId"),
        inverseJoinColumns = @JoinColumn(name = "childNodeId"))
    @OrderColumn(name = "idx")
    private List<PipelineDefinitionNode> nextNodes = new ArrayList<>();

    // Child node names are stored until post-processing can connect them to
    // the names of other nodes in the pipeline.
    @XmlAttribute(required = false)
    @Transient
    private String childNodeNames;

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineDefinitionNode_moduleParameterSetNames")
    private Map<ClassWrapper<ParametersInterface>, String> moduleParameterSetNames = new HashMap<>();

    @ManyToMany
    @JoinTable(name = "ziggy_PipelineDefinitionNode_inputDataFileTypes")
    private Set<DataFileType> inputDataFileTypes = new HashSet<>();

    @ManyToMany
    @JoinTable(name = "ziggy_PipelineDefinitionNode_outputDataFileTypes")
    private Set<DataFileType> outputDataFileTypes = new HashSet<>();

    @ManyToMany
    @JoinTable(name = "ziggy_PipelineDefinitionNode_modelTypes")
    private Set<ModelType> modelTypes = new HashSet<>();

    // This construct allows a node's module parameters, input and output data file types, and
    // model types to be entered in the XML file in any order. The downside of this flexibility
    // is that all of the elements wind up in the xmlReferences collection jumbled up together,
    // which complicates the bookkeeping of the various references.
    @XmlElements(
        value = { @XmlElement(name = "moduleParameter", type = ParameterSetReference.class),
            @XmlElement(name = "inputDataFileType", type = InputTypeReference.class),
            @XmlElement(name = "outputDataFileType", type = OutputTypeReference.class),
            @XmlElement(name = "modelType", type = ModelTypeReference.class) })
    @Transient
    private Set<XmlReference> xmlReferences = new HashSet<>();

    // Name of the PipelineDefinition instance for this object.
    private String pipelineName;

    @XmlAttribute(name = "singleSubtask", required = false)
    private Boolean singleSubtask = false;

    /*
     * Not stored in the database, but can be set for all nodes in a pipeline by calling
     * PipelineDefinition.buildPaths()
     */
    private transient PipelineDefinitionNode parentNode = null;
    private transient PipelineDefinitionNodePath path = null;

    public PipelineDefinitionNode() {
    }

    public PipelineDefinitionNode(String pipelineModuleDefinitionName,
        String pipelineDefinitionName) {
        moduleName = pipelineModuleDefinitionName;
        pipelineName = pipelineDefinitionName;
    }

    /**
     * Duplicates this node and all of its child nodes.
     */
    public PipelineDefinitionNode(PipelineDefinitionNode other) {
        maxWorkerCount = other.maxWorkerCount;
        heapSizeMb = other.heapSizeMb;

        moduleName = other.moduleName;

        for (PipelineDefinitionNode otherNode : other.nextNodes) {
            nextNodes.add(new PipelineDefinitionNode(otherNode));
        }

        for (Entry<ClassWrapper<ParametersInterface>, String> moduleParameterSetName : other.moduleParameterSetNames
            .entrySet()) {
            moduleParameterSetNames.put(moduleParameterSetName.getKey(),
                moduleParameterSetName.getValue());
        }

        inputDataFileTypes = new HashSet<>(other.inputDataFileTypes);
        outputDataFileTypes.addAll(other.outputDataFileTypes);
        modelTypes.addAll(other.modelTypes);
        pipelineName = other.pipelineName;
    }

    /**
     * populates the fields used by XML (specifically, {@link #xmlReferences} and
     * {@link #childNodeNames}) based on the contents of the instance that are provided from the
     * database. This ensures that, once retrieved from the database, the XML fields and database
     * fields are consistent with one another.
     * <p>
     * Note: when any method sets the value of a database field that is mirrored in the
     * {@link #xmlReferences} field, the {@link #xmlReferences} field must have all instances of the
     * appropriate object class removed via the
     * {@link CollectionFilters#removeTypeFromCollection(java.util.Collection, Class)} static
     * method. This is necessary to ensure that (a) the {@link #xmlReferences} field doesn't keep
     * any instances that are now obsolete due to the set operation, but (b) the instances of all
     * other classes in the {@link #xmlReferences} field are not removed when the instances from the
     * given class are removed.
     */
    public void populateXmlFields() {
        xmlReferences.addAll(inputDataFileTypes.stream()
            .map(s -> new InputTypeReference(s.getName()))
            .collect(Collectors.toSet()));
        xmlReferences.addAll(outputDataFileTypes.stream()
            .map(s -> new OutputTypeReference(s.getName()))
            .collect(Collectors.toSet()));
        xmlReferences.addAll(modelTypes.stream()
            .map(s -> new ModelTypeReference(s.getType()))
            .collect(Collectors.toSet()));
        xmlReferences.addAll(moduleParameterSetNames.values()
            .stream()
            .map(ParameterSetReference::new)
            .collect(Collectors.toSet()));

        // Use the setters to fill in the optional XML values.
        setMaxWorkerCount(getMaxWorkerCount());
        setHeapSizeMb(getHeapSizeMb());

        // We don't want to touch the childNodeNames String unless the nextNodes List is populated
        if (!nextNodes.isEmpty()) {
            setChildNodeNames();
        }
    }

    private void setChildNodeNames() {
        if (nextNodes.isEmpty()) {
            childNodeNames = null;
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (PipelineDefinitionNode node : nextNodes) {
            sb.append(node.getModuleName());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        childNodeNames = sb.toString();
    }

    public Long getId() {
        return id;
    }

    public List<PipelineDefinitionNode> getNextNodes() {
        return nextNodes;
    }

    public void setNextNodes(List<PipelineDefinitionNode> nextNodes) {
        this.nextNodes = nextNodes;
        populateXmlFields();
    }

    public void addNextNode(PipelineDefinitionNode nextNode) {
        if (nextNodes == null) {
            nextNodes = new ArrayList<>();
        }
        nextNodes.add(nextNode);
        populateXmlFields();
    }

    public PipelineDefinitionNode getParentNode() {
        return parentNode;
    }

    public void setParentNode(PipelineDefinitionNode parentNode) {
        this.parentNode = parentNode;
    }

    public Integer getMaxWorkerCount() {
        return maxWorkerCount;
    }

    public void setMaxWorkerCount(Integer workers) {
        maxWorkerCount = workers;
    }

    public Integer getHeapSizeMb() {
        return heapSizeMb;
    }

    public void setHeapSizeMb(Integer heapSizeMb) {
        this.heapSizeMb = heapSizeMb;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public Boolean getSingleSubtask() {
        return singleSubtask != null ? singleSubtask : false;
    }

    public void setSingleSubtask(Boolean singleSubtask) {
        this.singleSubtask = singleSubtask;
    }

    public void setPipelineModuleDefinition(PipelineModuleDefinition moduleDefinition) {
        moduleName = moduleDefinition.getName();
    }

    /**
     * Enforce the use of Java object identity so that we can use transient instances in a Set. This
     * of course means that non-transient instances with the same database id will not be equal(),
     * but this approach is safer because there's no chance that the equals/hashCode value will
     * change while it's contained in a Set, which would break the contract of Set.
     * <p>
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public PipelineDefinitionNodePath getPath() {
        return path;
    }

    public void setPath(PipelineDefinitionNodePath path) {
        this.path = path;
    }

    public Map<ClassWrapper<ParametersInterface>, String> getModuleParameterSetNames() {
        return moduleParameterSetNames;
    }

    public String putModuleParameterSetName(Class<? extends Parameters> clazz,
        String paramSetName) {
        ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(clazz);

        if (moduleParameterSetNames.containsKey(classWrapper)) {
            throw new PipelineException(
                "This TriggerDefinition already contains a pipeline parameter set name for class: "
                    + classWrapper);
        }
        populateXmlFields();
        return moduleParameterSetNames.put(classWrapper, paramSetName);
    }

    public void setModuleParameterSetNames(
        Map<ClassWrapper<ParametersInterface>, String> moduleParameterSetNames) {
        this.moduleParameterSetNames = moduleParameterSetNames;
        CollectionFilters.removeTypeFromCollection(xmlReferences, ParameterSetReference.class);
        populateXmlFields();
    }

    public Set<String> getParameterSetNames() {
        return CollectionFilters.filterToSet(xmlReferences, ParameterSetReference.class)
            .stream()
            .map(ParameterSetReference::getName)
            .collect(Collectors.toSet());
    }

    public void addInputDataFileType(DataFileType dataFileType) {
        inputDataFileTypes.add(dataFileType);
        populateXmlFields();
    }

    public void addAllInputDataFileTypes(Collection<DataFileType> dataFileTypes) {
        inputDataFileTypes.addAll(dataFileTypes);
        populateXmlFields();
    }

    public Set<DataFileType> getInputDataFileTypes() {
        return inputDataFileTypes;
    }

    public Set<InputTypeReference> getInputDataFileTypeReferences() {
        return CollectionFilters.filterToSet(xmlReferences, InputTypeReference.class);
    }

    public void addOutputDataFileType(DataFileType dataFileType) {
        outputDataFileTypes.add(dataFileType);
        populateXmlFields();
    }

    public void addAllOutputDataFileTypes(Set<DataFileType> dataFileTypes) {
        outputDataFileTypes.addAll(dataFileTypes);
        populateXmlFields();
    }

    public void setOutputDataFileTypes(Set<DataFileType> dataFileTypes) {
        outputDataFileTypes = dataFileTypes;
        CollectionFilters.removeTypeFromCollection(xmlReferences, OutputTypeReference.class);
        populateXmlFields();
    }

    public Set<DataFileType> getOutputDataFileTypes() {
        return outputDataFileTypes;
    }

    public Set<OutputTypeReference> getOutputDataFileTypeReferences() {
        return CollectionFilters.filterToSet(xmlReferences, OutputTypeReference.class);
    }

    public void setModelTypes(Set<ModelType> modelTypes) {
        this.modelTypes = modelTypes;
        CollectionFilters.removeTypeFromCollection(xmlReferences, ModelTypeReference.class);
        populateXmlFields();
    }

    public Set<ModelType> getModelTypes() {
        return modelTypes;
    }

    public void addModelType(ModelType modelType) {
        modelTypes.add(modelType);
        populateXmlFields();
    }

    public void addAllModelTypes(Set<ModelType> modelTypes) {
        this.modelTypes.addAll(modelTypes);
        populateXmlFields();
    }

    public Set<ModelTypeReference> getModelTypeReferences() {
        return CollectionFilters.filterToSet(xmlReferences, ModelTypeReference.class);
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineDefinitionName) {
        pipelineName = pipelineDefinitionName;
    }

    public String getChildNodeNames() {
        return childNodeNames;
    }

    /** For testing only */
    void setChildNodeNames(String childNodeIds) {
        childNodeNames = childNodeIds;
    }

    /** For testing only */
    void setXmlReferences(Collection<XmlReference> xmlReferences) {
        this.xmlReferences.addAll(xmlReferences);
    }

    void addXmlReference(XmlReference xmlReference) {
        xmlReferences.add(xmlReference);
    }
}

package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderColumn;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.hibernate.annotations.Cascade;

import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.xml.XmlReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.InputTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ModelTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.OutputTypeReference;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.CollectionFilters;

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
@Table(name = "PI_PIPELINE_DEF_NODE")
public class PipelineDefinitionNode {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_PDN_SEQ",
        allocationSize = 1)
    private long id;

    /**
     * Indicates whether the transition logic should simply propagate the
     * {@link UnitOfWorkGenerator} to the next node or whether it should check to see if all tasks
     * for the current node have completed, and if so, invoke the {@link UnitOfWorkGenerator} to
     * generate the tasks for the next node. See {@link PipelineExecutor.doTransition()}.
     * <p>
     * Note that the field is a Boolean rather than a boolean. This is because optional XML
     * attributes need to be class instances rather than primitives.
     */
    @XmlAttribute(required = false)
    private Boolean startNewUow = true;

    /**
     * If non-null, this UOW generator definition is used to generate the tasks for this node. May
     * only be null if startNewUow == false (in which case the task from the previous node is just
     * carried forward) and this is not the first node in a pipeline; or if the module for this node
     * has a default unit of work, generator, in which case the generator will be determined at
     * runtime.
     */
    @XmlAttribute(required = false, name = "uowGenerator")
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @Embedded
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "UOW_TG_CN")) })
    private ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator;

    @XmlAttribute(required = true, name = "moduleName")
    @XmlJavaTypeAdapter(ModuleName.ModuleNameAdapter.class)
    @ManyToOne(fetch = FetchType.EAGER)
    private ModuleName moduleName;

    @OneToMany(fetch = FetchType.EAGER)
    @Cascade({ org.hibernate.annotations.CascadeType.SAVE_UPDATE,
        org.hibernate.annotations.CascadeType.DETACH })
    @JoinTable(name = "PI_PDN_PDN", joinColumns = @JoinColumn(name = "PARENT_PI_PDN_ID"),
        inverseJoinColumns = @JoinColumn(name = "CHILD_PI_PDN_ID"))
    @OrderColumn(name = "IDX")
    private List<PipelineDefinitionNode> nextNodes = new ArrayList<>();

    // Child node names are stored until post-processing can connect them to
    // the names of other nodes in the pipeline.
    @XmlAttribute(required = false)
    @Transient
    private String childNodeNames;

    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(name = "PI_TDN_MPS")
    private Map<ClassWrapper<Parameters>, ParameterSetName> moduleParameterSetNames = new HashMap<>();

    @ManyToMany
    @JoinTable(name = "PI_PDN_DFT_INPUTS")
    private Set<DataFileType> inputDataFileTypes = new HashSet<>();

    @ManyToMany
    @JoinTable(name = "PI_PDN_DFT_OUTPUTS")
    private Set<DataFileType> outputDataFileTypes = new HashSet<>();

    @ManyToMany
    @JoinTable(name = "PI_PDN_MODEL_TYPES")
    private Set<ModelType> modelTypes = new HashSet<>();

    // This construct allows a node's module parameters, input and output data file types, and
    // model types to be entered in the XML file in any order. The downside of this flexibility
    // is that all of the elements wind up in the xmlReferences collection jumbled up together,
    // which complicates the bookkeeping of the various references.
    @XmlElements(value = { @XmlElement(name = "moduleParameter", type = ParameterSetName.class),
        @XmlElement(name = "inputDataFileType", type = InputTypeReference.class),
        @XmlElement(name = "outputDataFileType", type = OutputTypeReference.class),
        @XmlElement(name = "modelType", type = ModelTypeReference.class) })
    @Transient
    private Set<XmlReference> xmlReferences = new HashSet<>();

    // Name of the PipelineDefinition instance for this object.
    private String pipelineName;

    /*
     * Not stored in the database, but can be set for all nodes in a pipeline by calling
     * PipelineDefinition.buildPaths()
     */
    private transient PipelineDefinitionNode parentNode = null;
    private transient PipelineDefinitionNodePath path = null;

    public PipelineDefinitionNode() {
    }

    public PipelineDefinitionNode(ModuleName pipelineModuleDefinitionName,
        String pipelineDefinitionName) {
        moduleName = pipelineModuleDefinitionName;
        pipelineName = pipelineDefinitionName;
    }

    /**
     * Copy constructor Duplicate this node and all of its child nodes.
     *
     * @param other
     */
    public PipelineDefinitionNode(PipelineDefinitionNode other) {
        startNewUow = other.startNewUow;
        moduleName = other.moduleName;
        pipelineName = other.pipelineName;

        if (other.unitOfWorkGenerator != null) {
            unitOfWorkGenerator = new ClassWrapper<>(other.unitOfWorkGenerator);
        }

        for (PipelineDefinitionNode otherNode : other.nextNodes) {
            nextNodes.add(new PipelineDefinitionNode(otherNode));
        }
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
        xmlReferences.addAll(moduleParameterSetNames.values());

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
            sb.append(node.getModuleName().getName());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        childNodeNames = sb.toString();
    }

    /**
     * Find the task generator of the specified node. if startNewUow is false for this node, walk
     * back up the tree until with find a node with startNewUow == true Assumes that the back
     * pointers have been built with PipelineDefinition.buildPaths()
     *
     * @return
     */
    public PipelineDefinitionNode taskGeneratorNode() {
        PipelineDefinitionNode currentNode = this;

        while (!currentNode.isStartNewUow()) {
            currentNode = currentNode.getParentNode();
            if (currentNode == null) {
                throw new PipelineException(
                    "Configuration Error: Current node and all parent nodes have startNewUow == false");
            }
        }
        return currentNode;
    }

    /**
     * @return the id
     */
    public long getId() {
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

    public boolean isStartNewUow() {
        return startNewUow;
    }

    public void setStartNewUow(boolean startNewUow) {
        this.startNewUow = startNewUow;
    }

    public ClassWrapper<UnitOfWorkGenerator> getUnitOfWorkGenerator() {
        return unitOfWorkGenerator;
    }

    public void setUnitOfWorkGenerator(ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator) {
        this.unitOfWorkGenerator = unitOfWorkGenerator;
    }

    public ModuleName getModuleName() {
        return moduleName;
    }

    public void setModuleName(ModuleName moduleName) {
        this.moduleName = moduleName;
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

    public Map<ClassWrapper<Parameters>, ParameterSetName> getModuleParameterSetNames() {
        return moduleParameterSetNames;
    }

    public ParameterSetName putModuleParameterSetName(Class<? extends Parameters> clazz,
        ParameterSetName paramSetName) {
        ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(clazz);

        if (moduleParameterSetNames.containsKey(classWrapper)) {
            throw new PipelineException(
                "This TriggerDefinition already contains a pipeline parameter set name for class: "
                    + classWrapper);
        }
        populateXmlFields();
        return moduleParameterSetNames.put(classWrapper, paramSetName);
    }

    public void setModuleParameterSetNames(
        Map<ClassWrapper<Parameters>, ParameterSetName> moduleParameterSetNames) {
        this.moduleParameterSetNames = moduleParameterSetNames;
        CollectionFilters.removeTypeFromCollection(xmlReferences, ParameterSetName.class);
        populateXmlFields();
    }

    public Set<ParameterSetName> getParameterSetNames() {
        return CollectionFilters.filterToSet(xmlReferences, ParameterSetName.class);
    }

    public void addInputDataFileType(DataFileType dataFileType) {
        inputDataFileTypes.add(dataFileType);
        populateXmlFields();
    }

    public void addAllInputDataFileTypes(Set<DataFileType> dataFileTypes) {
        inputDataFileTypes.addAll(dataFileTypes);
        populateXmlFields();
    }

    public void setInputDataFileTypes(Set<DataFileType> dataFileTypes) {
        inputDataFileTypes = dataFileTypes;
        CollectionFilters.removeTypeFromCollection(xmlReferences, InputTypeReference.class);
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

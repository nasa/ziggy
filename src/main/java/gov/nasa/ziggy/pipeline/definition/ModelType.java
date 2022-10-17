package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.util.RegexBackslashManager;
import gov.nasa.ziggy.util.RegexGroupCounter;

/**
 * Defines a model type and properties of its file name convention.
 * <p>
 * A model file's naming convention is defined by a regular expression. This is used to determine
 * whether any given file is a model of the specified type.
 * <p>
 * Model names can include a timestamp. If no timestamp is included, one is prepended onto the file
 * name at import time.
 * <p>
 * Model files can include a version number. The version number can be either a simple positive
 * integer or else a semantic version number (M.n.p). If no version number is included in the
 * filename, a simple integer version number is prepended onto the file name at import time.
 *
 * @author Todd Klaus
 * @author PT
 */

@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_MODEL_TYPE")
public class ModelType implements Comparable<ModelType> {

    @XmlAttribute(required = true)
    @Id
    private String type;

    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(RegexBackslashManager.XmlRegexAdapter.class)
    private String fileNameRegex;

    // The following 3 fields use the boxed versions of int and boolean rather than the primitive
    // versions because JAXB can create an optional attribute from a boxed member but not a
    // primitive one. Seriously. Try it sometime.
    @XmlAttribute
    private Integer versionNumberGroup = -1;

    @XmlAttribute
    private Integer timestampGroup = -1;

    @XmlAttribute
    private Boolean semanticVersionNumber = false;

    @ProxyIgnore
    @Transient
    private Pattern pattern;

    // for hibernate use only
    public ModelType() {
    }

    /**
     * Ensures that the contents of the ModelType instance are internally consistent.
     */
    public void validate() {
        int groupCount = RegexGroupCounter.groupCount(fileNameRegex);
        if (versionNumberGroup > groupCount) {
            throw new IllegalStateException(
                "Version number group " + versionNumberGroup + " does not exist in model " + type);
        }
        if (timestampGroup > groupCount) {
            throw new IllegalStateException(
                "Timestamp group " + versionNumberGroup + " does not exist in model " + type);
        }
        if (versionNumberGroup <= 0 && semanticVersionNumber) {
            throw new IllegalStateException(
                "Timestamp group " + versionNumberGroup + " does not exist in model " + type);

        }
    }

    public Pattern pattern() {
        if (pattern == null) {
            pattern = Pattern.compile(fileNameRegex);
        }
        return pattern;
    }

    public boolean isThisModelType(String filename) {
        return pattern().matcher(filename).matches();
    }

    public String versionNumber(String filename) {
        Matcher matcher = pattern().matcher(filename);
        boolean matches = matcher.matches();
        if (matches && versionNumberGroup > 0) {
            return matcher.group(versionNumberGroup);
        } else {
            return "";
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFileNameRegex() {
        return fileNameRegex;
    }

    public void setFileNameRegex(String fileNameRegex) {
        this.fileNameRegex = fileNameRegex;
    }

    public int getVersionNumberGroup() {
        return versionNumberGroup;
    }

    public void setVersionNumberGroup(int versionNumberGroup) {
        this.versionNumberGroup = versionNumberGroup;
    }

    public int getTimestampGroup() {
        return timestampGroup;
    }

    public void setTimestampGroup(int timestampGroup) {
        this.timestampGroup = timestampGroup;
    }

    public boolean isSemanticVersionNumber() {
        return semanticVersionNumber;
    }

    public void setSemanticVersionNumber(boolean semanticVersionNumber) {
        this.semanticVersionNumber = semanticVersionNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ModelType other = (ModelType) obj;
        if (!Objects.equals(type, other.type)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ModelType o) {
        return type.compareTo(o.type);
    }
}

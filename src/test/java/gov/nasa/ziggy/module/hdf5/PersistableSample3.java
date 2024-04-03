package gov.nasa.ziggy.module.hdf5;

import java.util.Objects;

import gov.nasa.ziggy.module.io.Persistable;

/**
 * Test class that has only primitive scalar members.
 *
 * @author PT
 */
public class PersistableSample3 implements Persistable {

    private String stringVar = "This is a string";
    private int intVar = 5;
    private Integer boxedIntVar = 10;
    private boolean boolVar = true;
    private Boolean boxedBoolVar = Boolean.FALSE;
    private EnumTest enumScalar = EnumTest.FIRST;

    public String getStringVar() {
        return stringVar;
    }

    public void setStringVar(String stringVar) {
        this.stringVar = stringVar;
    }

    public int getIntVar() {
        return intVar;
    }

    public void setIntVar(int intVar) {
        this.intVar = intVar;
    }

    public Integer getBoxedIntVar() {
        return boxedIntVar;
    }

    public void setBoxedIntVar(Integer boxedIntVar) {
        this.boxedIntVar = boxedIntVar;
    }

    public boolean isBoolVar() {
        return boolVar;
    }

    public void setBoolVar(boolean boolVar) {
        this.boolVar = boolVar;
    }

    public Boolean getBoxedBoolVar() {
        return boxedBoolVar;
    }

    public void setBoxedBoolVar(Boolean boxedBoolVar) {
        this.boxedBoolVar = boxedBoolVar;
    }

    public EnumTest getEnumScalar() {
        return enumScalar;
    }

    public void setEnumScalar(EnumTest enumScalar) {
        this.enumScalar = enumScalar;
    }

    @Override
    public int hashCode() {
        return Objects.hash(boolVar, boxedBoolVar, boxedIntVar, enumScalar, intVar, stringVar);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PersistableSample3 other = (PersistableSample3) obj;
        if (boolVar != other.boolVar || !Objects.equals(boxedBoolVar, other.boxedBoolVar)
            || !Objects.equals(boxedIntVar, other.boxedIntVar) || enumScalar != other.enumScalar) {
            return false;
        }
        if (intVar != other.intVar || !Objects.equals(stringVar, other.stringVar)) {
            return false;
        }
        return true;
    }
}

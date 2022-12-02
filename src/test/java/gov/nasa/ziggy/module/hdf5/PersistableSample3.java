package gov.nasa.ziggy.module.hdf5;

import gov.nasa.ziggy.module.io.Persistable;

/**
 * Test class that has only primitive scalar members.
 *
 * @author PT
 */
public class PersistableSample3 implements Persistable {

    private String stringVar = "This is a string";
    private int intVar = 5;
    private Integer boxedIntVar = Integer.valueOf(10);
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
        final int prime = 31;
        int result = 1;
        result = prime * result + (boolVar ? 1231 : 1237);
        result = prime * result + (boxedBoolVar == null ? 0 : boxedBoolVar.hashCode());
        result = prime * result + (boxedIntVar == null ? 0 : boxedIntVar.hashCode());
        result = prime * result + (enumScalar == null ? 0 : enumScalar.hashCode());
        result = prime * result + intVar;
        result = prime * result + (stringVar == null ? 0 : stringVar.hashCode());
        return result;
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
        PersistableSample3 other = (PersistableSample3) obj;
        if (boolVar != other.boolVar) {
            return false;
        }
        if (boxedBoolVar == null) {
            if (other.boxedBoolVar != null) {
                return false;
            }
        } else if (!boxedBoolVar.equals(other.boxedBoolVar)) {
            return false;
        }
        if (boxedIntVar == null) {
            if (other.boxedIntVar != null) {
                return false;
            }
        } else if (!boxedIntVar.equals(other.boxedIntVar)) {
            return false;
        }
        if (enumScalar != other.enumScalar) {
            return false;
        }
        if (intVar != other.intVar) {
            return false;
        }
        if (stringVar == null) {
            if (other.stringVar != null) {
                return false;
            }
        } else if (!stringVar.equals(other.stringVar)) {
            return false;
        }
        return true;
    }

}

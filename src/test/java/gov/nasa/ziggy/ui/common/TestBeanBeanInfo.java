package gov.nasa.ziggy.ui.common;

import com.l2fprod.common.beans.BaseBeanInfo;
import com.l2fprod.common.beans.ExtendedPropertyDescriptor;

import gov.nasa.ziggy.ui.collections.ArrayPropertyEditor;
import gov.nasa.ziggy.util.TestBean;

public class TestBeanBeanInfo extends BaseBeanInfo {
    public TestBeanBeanInfo() {
        super(TestBean.class);

        ExtendedPropertyDescriptor desc;

        desc = addProperty("anInt");
        desc.setDisplayName("an integer");
        desc.setShortDescription("anInt desc");

        desc = addProperty("aFloat");
        desc.setDisplayName("a float ");
        desc.setShortDescription("aFloat desc");

        desc = addProperty("aString");
        desc.setDisplayName("a String");
        desc.setShortDescription("aString desc");

        desc = addProperty("anIntArray");
        desc.setDisplayName("an IntArray");
        desc.setShortDescription("anIntArray desc");
        desc.setPropertyEditorClass(ArrayPropertyEditor.class);

        desc = addProperty("aFloatArray");
        desc.setDisplayName("a FloatArray");
        desc.setShortDescription("aFloatArray desc");
        // desc.setPropertyEditorClass(ArrayPropertyEditor.class);
        // desc.setPropertyTableRendererClass(ArrayTableCellRenderer.class);

        desc = addProperty("aFloatArray2");
        desc.setDisplayName("a FloatArray2");
        desc.setShortDescription("aFloatArray2 desc");

        desc = addProperty("aStringArray");
        desc.setDisplayName("a StringArray");
        desc.setShortDescription("aStringArray desc");
        // desc.setPropertyEditorClass(ArrayPropertyEditor.class);

        desc = addProperty("aStringArrayNull");
        desc.setDisplayName("a StringArrayNull");
        desc.setShortDescription("aStringArrayNull desc");
    }
}

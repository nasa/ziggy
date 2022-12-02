package gov.nasa.ziggy.module.io;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation tells the MATLAB code generator and the serializers to ignore the annotated
 * field.
 *
 * @author Todd Klaus
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface ProxyIgnore {
}

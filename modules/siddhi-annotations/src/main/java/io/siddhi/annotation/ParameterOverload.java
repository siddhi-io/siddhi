package io.siddhi.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for storing the patterns of a Siddhi Extension.
 * <pre><code>
 * eg:-
 *      {@literal @}Extension(
 *                      ...
 *                       parameterOverloads = {
 *                           {@literal @}ParameterOverload(parameterNames={"firstParameterName","secondParameterName"}),
 *                           {@literal @}ParameterOverload(parameterNames={"firstParameterName"})
 *                       }
 *                      ...
 *      )
 *      public CustomExtension extends ExtensionSuperClass {
 *          ...
 *      }
 * </code></pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface ParameterOverload {
    String[] parameterNames() default {};
}

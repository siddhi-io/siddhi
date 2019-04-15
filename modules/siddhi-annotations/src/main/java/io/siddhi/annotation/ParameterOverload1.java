package io.siddhi.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for storing the parameters of a Siddhi Extension.
 * <pre><code>
 * eg:-
 *      {@literal @}Extension(
 *                      ...
 *                       parametersConstraints1 = {
 *                           {@literal @}ParameterOverload1(
 *                               parameters = {
 *                                   {@literal @}Parameter(name = "firstParameterName", type = {DataType.INT,
 *                                  DataType.LONG}),
 *                                   {@literal @}Parameter(name = "SecondParameterName", type = {DataType.STRING})
 *                               }
 *                            ),
 *                           {@literal @}ParameterOverload1(
 *                               parameters = {
 *                                   {@literal @}Parameter(name = "thirdParameterName", type = {DataType.STRING})
 *                               }
 *                           )
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
public @interface ParameterOverload1 {
    Parameter[] parameters() default {};

}
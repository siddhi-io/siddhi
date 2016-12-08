package org.wso2.siddhi.core.subscription;

import org.wso2.siddhi.query.api.definition.Attribute;

public class AttributeConverter {

    public static Object getPropertyValue(Object propertyValue, Attribute.Type attributeType) {

        if ((!Attribute.Type.STRING.equals(attributeType)) && propertyValue == null) {
            throw new RuntimeException("Found Invalid property value 'null' for attribute of type " + attributeType);
        }

        if (Attribute.Type.BOOL.equals(attributeType)) {
            return Boolean.parseBoolean(propertyValue.toString());
        } else if (Attribute.Type.DOUBLE.equals(attributeType)) {
            return Double.parseDouble(propertyValue.toString());
        } else if (Attribute.Type.FLOAT.equals(attributeType)) {
            return Float.parseFloat(propertyValue.toString());
        } else if (Attribute.Type.INT.equals(attributeType)) {
            return Integer.parseInt(propertyValue.toString());
        } else if (Attribute.Type.LONG.equals(attributeType)) {
            return Long.parseLong(propertyValue.toString());
        } else {
            return propertyValue == null ? null : propertyValue.toString();
        }
    }

}

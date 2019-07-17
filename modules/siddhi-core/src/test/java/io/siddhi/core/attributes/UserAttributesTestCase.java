package io.siddhi.core.attributes;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UserAttributesTestCase {

    private UserAttributes container;

    @BeforeMethod
    public void init() {
        container = new ConcurrentUserAttributes();
    }

    @Test
    public void testEmptyContainerBehaviour() {
        UserAttributeKey<Integer> anIntValue = new UserAttributeKey<>("attrInt");
        UserAttributeKey<String> aString = new UserAttributeKey<>("attrString");
        AssertJUnit.assertFalse(container.contains(aString));
        AssertJUnit.assertFalse(container.contains(anIntValue));
        AssertJUnit.assertNull(container.getOrNull(anIntValue));
        AssertJUnit.assertNull(container.getOrNull(aString));
        Assert.assertThrows(IllegalStateException.class, () -> container.get(aString));
        Assert.assertThrows(IllegalStateException.class, () -> container.get(anIntValue));
    }

    @Test
    public void testWithValuesBehaviour() {
        UserAttributeKey<Integer> anIntValue = new UserAttributeKey<>("attrInt");
        UserAttributeKey<String> aString = new UserAttributeKey<>("attrString");
        AssertJUnit.assertFalse(container.contains(aString));
        AssertJUnit.assertFalse(container.contains(anIntValue));

        container.put(anIntValue, 34);
        container.put(aString, "Test String");

        AssertJUnit.assertTrue(container.contains(aString));
        AssertJUnit.assertTrue(container.contains(anIntValue));

        AssertJUnit.assertEquals(container.get(aString), "Test String");
        AssertJUnit.assertEquals(container.get(anIntValue).intValue(), 34);

        AssertJUnit.assertEquals(container.remove(anIntValue).intValue(), 34);
        AssertJUnit.assertEquals(container.remove(aString), "Test String");

        AssertJUnit.assertFalse(container.contains(aString));
        AssertJUnit.assertFalse(container.contains(anIntValue));
    }

    @Test
    public void testDifferentKeyInstancesResolveCorrectly() {
        UserAttributeKey<Object> key1 = new UserAttributeKey<>("k1");
        UserAttributeKey<Object> key2 = new UserAttributeKey<>("k1");
        Object val = new Object();

        container.put(key1, val);
        AssertJUnit.assertEquals(container.get(key2), val);
    }
}

package com.example.awss3;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SampleInputTest {

    @Test
    public void requiredUsesArgumentWhenPresent() {
        String value = SampleInput.required(new String[]{"bucket-from-arg"}, 0, "AWS_S3_BUCKET", "bucket name");

        assertEquals("bucket-from-arg", value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void requiredThrowsWhenValueIsMissing() {
        SampleInput.required(new String[0], 0, "AWS_S3_BUCKET", "bucket name");
    }

    @Test
    public void optionalReturnsDefaultWhenArgumentIsMissing() {
        String value = SampleInput.optional(new String[0], 0, "AWS_S3_KEY", "default-key");

        assertEquals("default-key", value);
    }

    @Test
    public void optionalBooleanParsesBooleanArguments() {
        assertTrue(SampleInput.optionalBoolean(new String[]{"true"}, 0, "AWS_S3_CREATE_BUCKET", false));
        assertFalse(SampleInput.optionalBoolean(new String[]{"false"}, 0, "AWS_S3_CREATE_BUCKET", true));
    }
}

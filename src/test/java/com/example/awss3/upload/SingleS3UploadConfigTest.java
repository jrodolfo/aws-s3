package com.example.awss3.upload;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SingleS3UploadConfigTest {

    @Test
    public void resolveConfigUsesDefaultsForOptionalValues() {
        SingleS3Upload.Config config = SingleS3Upload.resolveConfig(new String[]{"bucket-a", "hello.txt"});

        assertEquals("bucket-a", config.bucket);
        assertEquals("hello.txt", config.key);
        assertEquals("Testing with the {sdk-java}", config.content);
        assertEquals(Region.US_EAST_2, config.region);
        assertFalse(config.createBucket);
        assertFalse(config.cleanUp);
    }

    @Test
    public void resolveConfigUsesExplicitOptionalArguments() {
        SingleS3Upload.Config config = SingleS3Upload.resolveConfig(
                new String[]{"bucket-a", "hello.txt", "payload", "us-east-1", "true", "true"});

        assertEquals("payload", config.content);
        assertEquals(Region.US_EAST_1, config.region);
        assertTrue(config.createBucket);
        assertTrue(config.cleanUp);
    }
}

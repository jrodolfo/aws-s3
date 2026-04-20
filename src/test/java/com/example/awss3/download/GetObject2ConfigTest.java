package com.example.awss3.download;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetObject2ConfigTest {

    @Test
    public void resolveConfigUsesDefaultsForOptionalValues() {
        GetObject2.Config config = GetObject2.resolveConfig(new String[]{"bucket-a", "hello.txt"});

        assertEquals("bucket-a", config.bucketName);
        assertEquals("hello.txt", config.key);
        assertEquals(Region.US_EAST_2, config.clientRegion);
        assertNull(config.profileName);
    }

    @Test
    public void resolveConfigUsesExplicitOptionalArguments() {
        GetObject2.Config config = GetObject2.resolveConfig(
                new String[]{"bucket-a", "hello.txt", "us-east-1", "default"});

        assertEquals(Region.US_EAST_1, config.clientRegion);
        assertEquals("default", config.profileName);
    }
}

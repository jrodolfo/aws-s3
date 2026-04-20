package com.example.awss3.upload;

import com.amazonaws.regions.Regions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MultiPartS3UploadConfigTest {

    @Test
    public void resolveConfigUsesFileNameAsDefaultKey() {
        MultiPartS3Upload.Config config = MultiPartS3Upload.resolveConfig(
                new String[]{"bucket-a", "/tmp/archive.zip"});

        assertEquals("bucket-a", config.bucketName);
        assertEquals("/tmp/archive.zip", config.filePath);
        assertEquals("archive.zip", config.keyName);
        assertEquals(Regions.US_EAST_2, config.region);
        assertEquals(10, config.maxUploadThreads);
        assertEquals(5242880L, config.uploadThreshold);
    }

    @Test
    public void resolveConfigUsesExplicitOptionalArguments() {
        MultiPartS3Upload.Config config = MultiPartS3Upload.resolveConfig(
                new String[]{"bucket-a", "/tmp/archive.zip", "s3-key.zip", "us-east-1", "4", "1024"});

        assertEquals("s3-key.zip", config.keyName);
        assertEquals(Regions.US_EAST_1, config.region);
        assertEquals(4, config.maxUploadThreads);
        assertEquals(1024L, config.uploadThreshold);
    }
}

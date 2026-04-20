package net.jrodolfo.awss3.integration;

import net.jrodolfo.awss3.upload.S3FileUpload;
import org.junit.Assume;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class S3IntegrationTest {

    @Test
    public void uploadsAndDownloadsATextObject() {
        String bucketName = System.getenv("AWS_S3_INTEGRATION_BUCKET");
        Assume.assumeTrue(bucketName != null && !bucketName.trim().isEmpty());

        String regionName = getEnvOrDefault("AWS_S3_INTEGRATION_REGION", Region.US_EAST_2.id());
        String profileName = System.getenv("AWS_S3_INTEGRATION_PROFILE");
        String key = "aws-s3-java-samples/integration-" + UUID.randomUUID() + ".txt";
        String content = "integration-test-" + UUID.randomUUID();

        try (S3Client s3Client = buildClient(regionName, profileName)) {
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    RequestBody.fromString(content));

            String downloadedContent = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build())
                    .asUtf8String();

            assertEquals(content, downloadedContent);

            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
        }
    }

    @Test
    public void uploadsAndDownloadsAFileObject() throws IOException {
        String bucketName = System.getenv("AWS_S3_INTEGRATION_BUCKET");
        Assume.assumeTrue(bucketName != null && !bucketName.trim().isEmpty());

        String regionName = getEnvOrDefault("AWS_S3_INTEGRATION_REGION", Region.US_EAST_2.id());
        String profileName = System.getenv("AWS_S3_INTEGRATION_PROFILE");
        String key = "aws-s3-java-samples/file-integration-" + UUID.randomUUID() + ".txt";
        String content = "file-integration-test-" + UUID.randomUUID();
        Path tempFile = Files.createTempFile("aws-s3-java-samples-", ".txt");
        Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));

        try (S3Client s3Client = buildClient(regionName, profileName)) {
            S3FileUpload.uploadFile(s3Client, tempFile, bucketName, key, 5L * 1024L * 1024L);

            String downloadedContent = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build())
                    .asUtf8String();

            assertEquals(content, downloadedContent);

            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private static S3Client buildClient(String regionName, String profileName) {
        S3ClientBuilder builder = S3Client.builder().region(Region.of(regionName));
        if (profileName == null || profileName.trim().isEmpty()) {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            builder.credentialsProvider(ProfileCredentialsProvider.create(profileName));
        }
        return builder.build();
    }

    private static String getEnvOrDefault(String envName, String defaultValue) {
        String value = System.getenv(envName);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value.trim();
    }
}

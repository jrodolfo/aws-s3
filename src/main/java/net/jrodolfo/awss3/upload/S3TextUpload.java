package net.jrodolfo.awss3.upload;

import net.jrodolfo.awss3.SampleInput;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;


public class S3TextUpload {

    public static void main(String[] args) {
        Config config = resolveConfig(args);

        S3Client s3 = S3Client.builder().region(config.region).build();
        try {
            if (config.createBucket) {
                tutorialSetup(s3, config.bucket, config.region);
            }

            System.out.println("Uploading object...");
            s3.putObject(PutObjectRequest.builder().bucket(config.bucket).key(config.key).build(),
                    RequestBody.fromString(config.content));
            System.out.println("Upload complete");
            System.out.printf("%n");

            if (config.cleanUp) {
                cleanUp(s3, config.bucket, config.key);
            }
        } finally {
            System.out.println("Closing the connection to {S3}");
            s3.close();
            System.out.println("Connection closed");
            System.out.println("Exiting...");
        }
    }

    static Config resolveConfig(String[] args) {
        String bucket = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String key = SampleInput.required(args, 1, "AWS_S3_KEY", "object key");
        String content = SampleInput.optional(args, 2, "AWS_S3_CONTENT", "Testing with the {sdk-java}");
        Region region = Region.of(SampleInput.optional(args, 3, "AWS_S3_REGION", Region.US_EAST_2.id()));
        boolean createBucket = SampleInput.optionalBoolean(args, 4, "AWS_S3_CREATE_BUCKET", false);
        boolean cleanUp = SampleInput.optionalBoolean(args, 5, "AWS_S3_CLEANUP", false);
        return new Config(bucket, key, content, region, createBucket, cleanUp);
    }

    public static void tutorialSetup(S3Client s3Client, String bucketName, Region region) {
        try {
            CreateBucketRequest.Builder requestBuilder = CreateBucketRequest.builder().bucket(bucketName);
            if (!Region.US_EAST_1.equals(region)) {
                requestBuilder.createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
                                .build());
            }

            s3Client.createBucket(requestBuilder.build());
            System.out.println("Creating bucket: " + bucketName);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println(bucketName + " is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object: " + keyName);
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println(keyName + " has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }

    static final class Config {
        final String bucket;
        final String key;
        final String content;
        final Region region;
        final boolean createBucket;
        final boolean cleanUp;

        Config(String bucket, String key, String content, Region region, boolean createBucket, boolean cleanUp) {
            this.bucket = bucket;
            this.key = key;
            this.content = content;
            this.region = region;
            this.createBucket = createBucket;
            this.cleanUp = cleanUp;
        }
    }
}

package com.example.awss3.download;

import com.example.awss3.SampleInput;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetObject2 {

    public static void main(String[] args) throws IOException {
        Config config = resolveConfig(args);

        try (S3Client s3Client = buildClient(config)) {
            // 1) Get an object and print its contents
            System.out.println("Downloading an object");
            try (ResponseInputStream<GetObjectResponse> fullObject = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(config.bucketName)
                    .key(config.key)
                    .build())) {
                System.out.println("Content-Type: " + fullObject.response().contentType());
                System.out.println("Content: ");
                displayTextInputStream(fullObject);
            }

            // 2) Get a range of bytes from an object and print the bytes
            try (ResponseInputStream<GetObjectResponse> objectPortion = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(config.bucketName)
                    .key(config.key)
                    .range("bytes=0-9")
                    .build())) {
                System.out.println("Printing bytes retrieved.");
                displayTextInputStream(objectPortion);
            }

            // 3) Get an entire object, overriding the specified response headers, and print the object's content
            try (ResponseInputStream<GetObjectResponse> headerOverrideObject = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(config.bucketName)
                    .key(config.key)
                    .responseCacheControl("No-cache")
                    .responseContentDisposition("attachment; filename=example.txt")
                    .build())) {
                displayTextInputStream(headerOverrideObject);
            }
        } catch (NoSuchKeyException e) {
            e.printStackTrace();
        } catch (S3Exception e) {
            e.printStackTrace();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    static Config resolveConfig(String[] args) {
        Region clientRegion = Region.of(
                SampleInput.optional(args, 2, "AWS_S3_REGION", Region.US_EAST_2.id()));
        String bucketName = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String key = SampleInput.required(args, 1, "AWS_S3_KEY", "object key");
        String profileName = SampleInput.optional(args, 3, "AWS_PROFILE", null);
        return new Config(bucketName, key, clientRegion, profileName);
    }

    private static S3Client buildClient(Config config) {
        S3ClientBuilder builder = S3Client.builder().region(config.clientRegion);
        if (config.profileName == null) {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            builder.credentialsProvider(ProfileCredentialsProvider.create(config.profileName));
        }
        return builder.build();
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

    static final class Config {
        final String bucketName;
        final String key;
        final Region clientRegion;
        final String profileName;

        Config(String bucketName, String key, Region clientRegion, String profileName) {
            this.bucketName = bucketName;
            this.key = key;
            this.clientRegion = clientRegion;
            this.profileName = profileName;
        }
    }
}

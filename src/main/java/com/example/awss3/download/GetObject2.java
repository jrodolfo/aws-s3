package com.example.awss3.download;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.example.awss3.SampleInput;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetObject2 {

    public static void main(String[] args) throws IOException {
        Config config = resolveConfig(args);

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AWSCredentialsProvider credentialsProvider = config.profileName == null
                    ? new DefaultAWSCredentialsProviderChain()
                    : new ProfileCredentialsProvider(config.profileName);
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(config.clientRegion)
                    .withCredentials(credentialsProvider)
                    .build();

            // 1) Get an object and print its contents
            System.out.println("Downloading an object");
            fullObject = s3Client.getObject(new GetObjectRequest(config.bucketName, config.key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            System.out.println("Content: ");
            displayTextInputStream(fullObject.getObjectContent());

            // 2) Get a range of bytes from an object and print the bytes
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(config.bucketName, config.key).withRange(0, 9);
            objectPortion = s3Client.getObject(rangeObjectRequest);
            System.out.println("Printing bytes retrieved.");
            displayTextInputStream(objectPortion.getObjectContent());

            // 3) Get an entire object, overriding the specified response headers, and print the object's content
            ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");
            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(config.bucketName, config.key)
                    .withResponseHeaders(headerOverrides);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            displayTextInputStream(headerOverrideObject.getObjectContent());

        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }
    }

    static Config resolveConfig(String[] args) {
        Regions clientRegion = Regions.fromName(
                SampleInput.optional(args, 2, "AWS_S3_REGION", Regions.US_EAST_2.getName()));
        String bucketName = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String key = SampleInput.required(args, 1, "AWS_S3_KEY", "object key");
        String profileName = SampleInput.optional(args, 3, "AWS_PROFILE", null);
        return new Config(bucketName, key, clientRegion, profileName);
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
        final Regions clientRegion;
        final String profileName;

        Config(String bucketName, String key, Regions clientRegion, String profileName) {
            this.bucketName = bucketName;
            this.key = key;
            this.clientRegion = clientRegion;
            this.profileName = profileName;
        }
    }
}

package com.example.awss3.upload;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.example.awss3.SampleInput;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.Executors;

public class MultiPartS3Upload {

    public static void main(String[] args) throws Exception {
        Config config = resolveConfig(args);

        Path path = Paths.get(config.filePath);
        long fileSizeInBytes = Files.size(path);

        System.out.println("Uploading file " + config.filePath +
                ", size " + fileSizeInBytes + " bytes, " + "to the AWS S3 bucket " + config.bucketName + ".");

        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(config.region)
                .build();

        TransferManager transferManager = TransferManagerBuilder
                .standard()
                .withS3Client(s3Client)
                .withMultipartUploadThreshold(config.uploadThreshold)
                .withExecutorFactory(() -> Executors.newFixedThreadPool(config.maxUploadThreads))
                .build();

        PutObjectRequest request = new PutObjectRequest(config.bucketName, config.keyName, new File(config.filePath));
        Upload upload = transferManager.upload(request);
        upload.addProgressListener(createProgressListener(upload));

        try {
            upload.waitForCompletion();
            Calendar calendar = Calendar.getInstance();
            SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
            System.out.println(formatter.format(calendar.getTime()) + " - Upload is completed.");
        } catch (AmazonClientException e) {
            System.err.println("An error occurred while uploading the file " + config.filePath +
                    " to the AWS S3 bucket " + config.bucketName + ".");
            e.printStackTrace();
            throw e;
        } finally {
            transferManager.shutdownNow(false);
        }
    }

    static Config resolveConfig(String[] args) {
        String filePath = SampleInput.required(args, 1, "AWS_S3_FILE", "file path");
        String bucketName = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String keyName = SampleInput.optional(args, 2, "AWS_S3_KEY",
                Paths.get(filePath).getFileName().toString());
        Regions region = Regions.fromName(SampleInput.optional(args, 3, "AWS_S3_REGION", Regions.US_EAST_2.getName()));
        int maxUploadThreads = Integer.parseInt(SampleInput.optional(args, 4, "AWS_S3_MAX_THREADS", "10"));
        long uploadThreshold = Long.parseLong(SampleInput.optional(args, 5, "AWS_S3_MULTIPART_THRESHOLD", "5242880"));
        return new Config(bucketName, filePath, keyName, region, maxUploadThreads, uploadThreshold);
    }

    private static ProgressListener createProgressListener(Transfer transfer) {
        return new ProgressListener() {
            private double previousTransferred;

            @Override
            public synchronized void progressChanged(ProgressEvent event) {
                double transferred = transfer.getProgress().getPercentTransferred();
                Calendar calendar;
                SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
                if (transferred >= (previousTransferred + 10.0)) {
                    calendar = Calendar.getInstance();
                    System.out.println(formatter.format(calendar.getTime()) + " - Upload percentage: " +
                            new DecimalFormat("#.#").format(transferred) + "%");
                    previousTransferred = transferred;
                }
            }
        };
    }

    static final class Config {
        final String bucketName;
        final String filePath;
        final String keyName;
        final Regions region;
        final int maxUploadThreads;
        final long uploadThreshold;

        Config(String bucketName, String filePath, String keyName, Regions region, int maxUploadThreads,
               long uploadThreshold) {
            this.bucketName = bucketName;
            this.filePath = filePath;
            this.keyName = keyName;
            this.region = region;
            this.maxUploadThreads = maxUploadThreads;
            this.uploadThreshold = uploadThreshold;
        }
    }
}

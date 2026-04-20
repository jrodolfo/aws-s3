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
        String bucketName = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String absolutePathWithFileName = SampleInput.required(args, 1, "AWS_S3_FILE", "file path");
        String keyName = SampleInput.optional(args, 2, "AWS_S3_KEY",
                Paths.get(absolutePathWithFileName).getFileName().toString());
        Regions region = Regions.fromName(SampleInput.optional(args, 3, "AWS_S3_REGION", Regions.US_EAST_2.getName()));
        int maxUploadThreads = Integer.parseInt(SampleInput.optional(args, 4, "AWS_S3_MAX_THREADS", "10"));
        long uploadThreshold = Long.parseLong(SampleInput.optional(args, 5, "AWS_S3_MULTIPART_THRESHOLD", "5242880"));

        Path path = Paths.get(absolutePathWithFileName);
        long fileSizeInBytes = Files.size(path);

        System.out.println("Uploading file " + absolutePathWithFileName +
                ", size " + fileSizeInBytes + " bytes, " + "to the AWS S3 bucket " + bucketName + ".");

        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();

        TransferManager transferManager = TransferManagerBuilder
                .standard()
                .withS3Client(s3Client)
                .withMultipartUploadThreshold(uploadThreshold)
                .withExecutorFactory(() -> Executors.newFixedThreadPool(maxUploadThreads))
                .build();

        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(absolutePathWithFileName));
        Upload upload = transferManager.upload(request);
        upload.addProgressListener(createProgressListener(upload));

        try {
            upload.waitForCompletion();
            Calendar calendar = Calendar.getInstance();
            SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
            System.out.println(formatter.format(calendar.getTime()) + " - Upload is completed.");
        } catch (AmazonClientException e) {
            System.err.println("An error occurred while uploading the file " + absolutePathWithFileName +
                    " to the AWS S3 bucket " + bucketName + ".");
            e.printStackTrace();
            throw e;
        } finally {
            transferManager.shutdownNow(false);
        }
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
}

package com.example.awss3.upload;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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

        String bucketName = "jrodolfo-aws-training";
        String keyName = "jdk-8u333-windows-x64.exe";
        String fileName = "jdk-8u333-windows-x64.exe";

        String absolutePathWithFileName = "C:\\dev\\doc\\" + fileName;
        Path path = Paths.get(absolutePathWithFileName);
        long fileSizeInBytes = Files.size(path);
        int maxUploadThreads = 10;
        long uploadThreshold = 5 * 1024 * 1024;

        System.out.println("Uploading file " + absolutePathWithFileName +
                ", size " + fileSizeInBytes + " bytes, " + "to the AWS S3 bucket " + bucketName + ".");

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(Regions.US_EAST_2)
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
            System.exit(0);
        } catch (AmazonClientException e) {
            System.err.println("An error occurred while uploading the file " + absolutePathWithFileName +
                    "to the AWS S3 bucket " + bucketName + ".");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static ProgressListener createProgressListener(Transfer transfer)
    {
        return new ProgressListener()
        {
            private double previousTransferred;

            @Override
            public synchronized void progressChanged(ProgressEvent event)
            {
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

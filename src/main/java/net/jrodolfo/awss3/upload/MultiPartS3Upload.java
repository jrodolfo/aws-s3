package net.jrodolfo.awss3.upload;

import net.jrodolfo.awss3.SampleInput;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.List;

public class MultiPartS3Upload {
    private static final long MIN_MULTIPART_PART_SIZE = 5L * 1024L * 1024L;

    public static void main(String[] args) throws Exception {
        Config config = resolveConfig(args);

        Path path = Paths.get(config.filePath);
        long fileSizeInBytes = Files.size(path);

        System.out.println("Uploading file " + config.filePath +
                ", size " + fileSizeInBytes + " bytes, " + "to the AWS S3 bucket " + config.bucketName + ".");

        try (S3Client s3Client = S3Client.builder().region(config.region).build()) {
            uploadFile(s3Client, path, fileSizeInBytes, config);
        }
    }

    static Config resolveConfig(String[] args) {
        String filePath = SampleInput.required(args, 1, "AWS_S3_FILE", "file path");
        String bucketName = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String keyName = SampleInput.optional(args, 2, "AWS_S3_KEY",
                Paths.get(filePath).getFileName().toString());
        Region region = Region.of(SampleInput.optional(args, 3, "AWS_S3_REGION", Region.US_EAST_2.id()));
        int maxUploadThreads = Integer.parseInt(SampleInput.optional(args, 4, "AWS_S3_MAX_THREADS", "10"));
        long multipartThreshold = Long.parseLong(SampleInput.optional(args, 5, "AWS_S3_MULTIPART_THRESHOLD", "5242880"));
        return new Config(bucketName, filePath, keyName, region, maxUploadThreads, multipartThreshold);
    }

    private static void uploadFile(S3Client s3Client, Path path, long fileSizeInBytes, Config config) throws IOException {
        if (fileSizeInBytes <= config.multipartThreshold) {
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(config.bucketName)
                            .key(config.keyName)
                            .build(),
                    RequestBody.fromFile(path));
            logCompletion();
            return;
        }

        String uploadId = null;
        try {
            CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(config.bucketName)
                            .key(config.keyName)
                            .build());
            uploadId = createMultipartUploadResponse.uploadId();

            long partSize = Math.max(config.multipartThreshold, MIN_MULTIPART_PART_SIZE);
            List<CompletedPart> completedParts = new ArrayList<>();
            long uploadedBytes = 0L;
            int partNumber = 1;

            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
                while (uploadedBytes < fileSizeInBytes) {
                    long currentPartSize = Math.min(partSize, fileSizeInBytes - uploadedBytes);
                    byte[] partBytes = new byte[(int) currentPartSize];
                    file.readFully(partBytes);

                    UploadPartResponse uploadPartResponse = s3Client.uploadPart(
                            UploadPartRequest.builder()
                                    .bucket(config.bucketName)
                                    .key(config.keyName)
                                    .uploadId(uploadId)
                                    .partNumber(partNumber)
                                    .contentLength(currentPartSize)
                                    .build(),
                            RequestBody.fromBytes(partBytes));

                    completedParts.add(CompletedPart.builder()
                            .partNumber(partNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build());

                    uploadedBytes += currentPartSize;
                    logProgress(uploadedBytes, fileSizeInBytes);
                    partNumber++;
                }
            }

            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(config.bucketName)
                    .key(config.keyName)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build());
            logCompletion();
        } catch (S3Exception | IOException e) {
            if (uploadId != null) {
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(config.bucketName)
                        .key(config.keyName)
                        .uploadId(uploadId)
                        .build());
            }
            throw e;
        };
    }

    private static void logProgress(long uploadedBytes, long totalBytes) {
        double transferred = (uploadedBytes * 100.0) / totalBytes;
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        System.out.println(formatter.format(calendar.getTime()) + " - Upload percentage: " +
                new DecimalFormat("#.#").format(transferred) + "%");
    }

    private static void logCompletion() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        System.out.println(formatter.format(calendar.getTime()) + " - Upload is completed.");
    }

    static final class Config {
        final String bucketName;
        final String filePath;
        final String keyName;
        final Region region;
        final int maxUploadThreads;
        final long multipartThreshold;

        Config(String bucketName, String filePath, String keyName, Region region, int maxUploadThreads,
               long multipartThreshold) {
            this.bucketName = bucketName;
            this.filePath = filePath;
            this.keyName = keyName;
            this.region = region;
            this.maxUploadThreads = maxUploadThreads;
            this.multipartThreshold = multipartThreshold;
        }
    }
}

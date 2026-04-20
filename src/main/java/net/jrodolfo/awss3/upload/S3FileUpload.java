package net.jrodolfo.awss3.upload;

import net.jrodolfo.awss3.SampleInput;
import software.amazon.awssdk.core.exception.SdkClientException;
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

public class S3FileUpload {
    private static final long MIN_MULTIPART_PART_SIZE = 5L * 1024L * 1024L;

    public static void main(String[] args) throws Exception {
        try {
            Config config = resolveConfig(args);
            try (S3Client s3Client = S3Client.builder().region(config.region).build()) {
                uploadFile(s3Client, Paths.get(config.filePath), config.bucketName, config.keyName, config.multipartThreshold);
            }
        } catch (IllegalArgumentException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            fail("Unable to read the local file. Check that the path exists and is readable. " + e.getMessage());
        } catch (S3Exception e) {
            fail("S3 upload failed. " + awsMessage(e) + " Check the bucket name, region, object key, and your permissions.");
        } catch (SdkClientException e) {
            fail("Unable to reach AWS S3. Check your credentials, AWS profile, network access, and region. "
                    + e.getMessage());
        }
    }

    static Config resolveConfig(String[] args) {
        String filePath = SampleInput.required(args, 1, "AWS_S3_FILE", "file path");
        String bucketName = SampleInput.required(args, 0, "AWS_S3_BUCKET", "bucket name");
        String keyName = SampleInput.optional(args, 2, "AWS_S3_KEY",
                Paths.get(filePath).getFileName().toString());
        Region region = Region.of(SampleInput.optional(args, 3, "AWS_S3_REGION", Region.US_EAST_2.id()));
        long multipartThreshold = Long.parseLong(SampleInput.optional(args, 4, "AWS_S3_MULTIPART_THRESHOLD", "5242880"));
        return new Config(bucketName, filePath, keyName, region, multipartThreshold);
    }

    public static void uploadFile(S3Client s3Client, Path path, String bucketName, String keyName,
                                  long multipartThreshold) throws IOException {
        long fileSizeInBytes = Files.size(path);

        System.out.println("Uploading file " + path +
                ", size " + fileSizeInBytes + " bytes, " + "to the AWS S3 bucket " + bucketName + ".");

        if (fileSizeInBytes <= multipartThreshold) {
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(keyName)
                            .build(),
                    RequestBody.fromFile(path));
            logCompletion();
            return;
        }

        String uploadId = null;
        try {
            CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(keyName)
                            .build());
            uploadId = createMultipartUploadResponse.uploadId();

            long partSize = Math.max(multipartThreshold, MIN_MULTIPART_PART_SIZE);
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
                                    .bucket(bucketName)
                                    .key(keyName)
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
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build());
            logCompletion();
        } catch (S3Exception | IOException e) {
            if (uploadId != null) {
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
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

    private static String awsMessage(S3Exception e) {
        return e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage();
    }

    private static void fail(String message) {
        System.err.println("Error: " + message);
        System.exit(1);
    }

    static final class Config {
        final String bucketName;
        final String filePath;
        final String keyName;
        final Region region;
        final long multipartThreshold;

        Config(String bucketName, String filePath, String keyName, Region region, long multipartThreshold) {
            this.bucketName = bucketName;
            this.filePath = filePath;
            this.keyName = keyName;
            this.region = region;
            this.multipartThreshold = multipartThreshold;
        }
    }
}

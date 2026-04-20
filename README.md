# AWS S3 Java Samples

This repository contains small Java examples for uploading to and downloading from Amazon S3 using the AWS SDK for Java.

The project is organized as sample entry points rather than a production-ready application. The project currently uses examples built with two versions of the AWS SDK for Java: the older v1 SDK and the newer v2 SDK:

- `SingleS3Upload`: uploads a small string object with AWS SDK v2.
- `MultiPartS3Upload`: uploads a file with multipart transfer support using AWS SDK v1.
- `GetObject2`: downloads an object and demonstrates a few retrieval options using AWS SDK v1.

## Project Layout

```text
src/main/java/com/example/awss3/upload/SingleS3Upload.java
src/main/java/com/example/awss3/upload/MultiPartS3Upload.java
src/main/java/com/example/awss3/download/GetObject2.java
src/test/java/com/example/awss3/SingleS3UploadTest.java
```

## Requirements

- Java 8 or later
- Maven 3.6 or later
- An AWS account with access to Amazon S3
- AWS credentials available locally through the default credential chain or an AWS profile

## Build

```bash
mvn test
```

The included test suite is only a placeholder smoke test. It does not exercise real S3 operations.

## AWS Credentials

The examples rely on standard AWS credential resolution:

- `SingleS3Upload` uses the AWS SDK v2 default credential provider chain.
- `MultiPartS3Upload` uses the AWS SDK v1 `DefaultAWSCredentialsProviderChain`.
- `GetObject2` uses the AWS SDK v1 `ProfileCredentialsProvider`.

One common local setup is:

```bash
aws configure
```

You should also review the region and bucket values in each sample before running them.

## Running the Samples

You can run the examples with Maven:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.awss3.upload.SingleS3Upload
mvn -q exec:java -Dexec.mainClass=com.example.awss3.upload.MultiPartS3Upload
mvn -q exec:java -Dexec.mainClass=com.example.awss3.download.GetObject2
```

If the Maven Exec plugin is not configured in your local environment, you can also run the classes from your IDE.

## Sample Notes

### `SingleS3Upload`

- Uses region `us-east-2`.
- Creates a bucket with a timestamp-based name.
- Uploads a small in-memory string to S3.
- Includes a cleanup helper, but cleanup is currently commented out in `main`.

This sample is the closest thing in the repository to a self-contained example.

### `MultiPartS3Upload`

- Uses AWS SDK v1 `TransferManager`.
- Assumes a pre-existing bucket named `jrodolfo-aws-training`.
- Assumes a local Windows file path: `C:\dev\doc\jdk-8u333-windows-x64.exe`.
- Uses region `us-east-2`.

This sample will need code changes before it runs in most environments.

### `GetObject2`

- Uses AWS SDK v1 `AmazonS3`.
- Assumes bucket `jrodolfo-aws-training`.
- Assumes key `test.txt`.
- Demonstrates full-object download, ranged download, and response header overrides.

This sample also requires you to update the hard-coded bucket, key, and credential/profile expectations for your environment.

## Dependencies

The project currently depends on:

- AWS SDK for Java v2 S3 client
- AWS SDK for Java v1
- Commons IO
- JUnit 4

## References

- [AWS SDK for Java 2.x developer guide](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html)
- [Amazon S3 object download examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html)
- [Multipart uploads in Amazon S3 with Java](https://www.baeldung.com/aws-s3-multipart-upload)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

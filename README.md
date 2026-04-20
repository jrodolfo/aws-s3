# AWS S3 Java Samples

[![CI](https://github.com/jrodolfo/aws-s3/actions/workflows/ci.yml/badge.svg)](https://github.com/jrodolfo/aws-s3/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)
[![Java](https://img.shields.io/badge/java-8%2B-blue.svg)](https://www.java.com/)

This repository is a small reference project for uploading to and downloading from Amazon S3 with Java.

It is not a production-ready library. It is a compact set of S3 examples you can read, run, and adapt.

## What Is Here

- `S3TextUpload`: upload a small text object to S3
- `S3FileUpload`: upload a local file with S3 multipart upload behavior
- `S3ObjectDownload`: download an object and demonstrate a few retrieval options

All samples use the AWS SDK for Java v2 and follow the same runtime configuration pattern.

## Quick Start

Requirements:

- Java 8 or later
- Maven 3.6 or later
- an AWS account with S3 access
- AWS credentials available locally

A typical local setup is:

```bash
aws configure
```

For a quick local starting point, see [.env.example](./.env.example). The repository does not load `.env` files automatically, but the file shows the variables you may want to export before running the samples.

The fastest way to try the repository end to end is:

```bash
make roundtrip-object BUCKET=my-s3-sample-bucket KEY=hello.txt CONTENT="hello from make"
```

If the bucket does not exist yet:

```bash
make roundtrip-object BUCKET=my-s3-sample-bucket KEY=hello.txt CONTENT="hello from make" CREATE_BUCKET=true
```

That flow uploads a text object and then downloads the same object immediately after.

## Project Layout

```text
Makefile
LICENSE
README.md
.env.example
.github/
  workflows/
    ci.yml
scripts/
  upload-text.sh
  upload-file.sh
  download-object.sh
  roundtrip-object.sh
src/
  main/java/net/jrodolfo/awss3/
    SampleInput.java
    upload/
      S3TextUpload.java
      S3FileUpload.java
    download/
      S3ObjectDownload.java
  test/java/net/jrodolfo/awss3/
    SampleInputTest.java
    integration/
      S3IntegrationTest.java
    upload/
      S3TextUploadConfigTest.java
      S3FileUploadConfigTest.java
    download/
      S3ObjectDownloadConfigTest.java
```

## Common Commands

The most useful entry point is `make`:

```bash
make help
make test
make upload-text BUCKET=my-bucket KEY=hello.txt CONTENT="hello from make"
make upload-file BUCKET=my-bucket FILE=/path/to/file.zip
make download-object BUCKET=my-bucket KEY=hello.txt
make roundtrip-object BUCKET=my-bucket KEY=hello.txt CONTENT="hello from make"
make test-integration INTEGRATION_BUCKET=my-s3-sample-bucket
```

The repository also includes direct shell wrappers:

- `scripts/upload-text.sh`
- `scripts/upload-file.sh`
- `scripts/download-object.sh`
- `scripts/roundtrip-object.sh`

Each script supports `--help`.

## Runtime Configuration

Each sample follows the same rule:

- positional command-line arguments take priority
- otherwise the sample checks environment variables
- if a required value is still missing, the sample exits with a clear error

Common environment variables:

- `AWS_S3_BUCKET`
- `AWS_S3_KEY`
- `AWS_S3_REGION`
- `AWS_S3_FILE`
- `AWS_S3_CONTENT`
- `AWS_S3_CREATE_BUCKET`
- `AWS_S3_CLEANUP`
- `AWS_S3_MULTIPART_THRESHOLD`
- `AWS_PROFILE`

## Direct Entry Points

Use these if you want to run the Java classes directly with Maven instead of `make` or the shell scripts.

### Text Upload

Arguments:

1. `bucket`
2. `key`
3. `content` optional, default `Testing with the {sdk-java}`
4. `region` optional, default `us-east-2`
5. `createBucket` optional, default `false`
6. `cleanup` optional, default `false`

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=net.jrodolfo.awss3.upload.S3TextUpload \
  -Dexec.args="my-s3-sample-bucket hello.txt 'hello from java' us-east-2 false false"
```

### File Upload

Arguments:

1. `bucket`
2. `filePath`
3. `key` optional, default local file name
4. `region` optional, default `us-east-2`
5. `multipartThresholdBytes` optional, default `5242880`

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=net.jrodolfo.awss3.upload.S3FileUpload \
  -Dexec.args="my-s3-sample-bucket /path/to/file.zip file.zip us-east-2 5242880"
```

### Object Download

Arguments:

1. `bucket`
2. `key`
3. `region` optional, default `us-east-2`
4. `profile` optional

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=net.jrodolfo.awss3.download.S3ObjectDownload \
  -Dexec.args="my-s3-sample-bucket hello.txt us-east-2 default"
```

## Integration Test

The repository includes an opt-in integration test that performs a real S3 round trip:

- upload and download a temporary text object
- upload and download a temporary local file
- verify the downloaded content matches
- delete the temporary objects

Normal `mvn test` runs stay lightweight. The integration test is skipped unless `AWS_S3_INTEGRATION_BUCKET` is set.

Run it directly:

```bash
AWS_S3_INTEGRATION_BUCKET=my-s3-sample-bucket \
mvn -Dtest=net.jrodolfo.awss3.integration.S3IntegrationTest test
```

Or through `make`:

```bash
make test-integration INTEGRATION_BUCKET=my-s3-sample-bucket
```

Optional integration variables:

- `AWS_S3_INTEGRATION_REGION`, default `us-east-2`
- `AWS_S3_INTEGRATION_PROFILE`, optional named AWS profile

## Build And CI

Local build:

```bash
mvn test
```

The test suite covers runtime input parsing and sample configuration behavior. It does not run real S3 operations unless you explicitly enable the integration test.

GitHub Actions runs `mvn test` automatically on pushes to `main` and on pull requests. The workflow file lives at `.github/workflows/ci.yml`.

## Troubleshooting

- `Error: Unable to reach AWS S3...`
  Check your AWS credentials, active profile, network access, and region.
- `Error: S3 request failed...`
  Confirm the bucket, object key, region, and IAM permissions for the operation.
- `Error: Object not found in S3...`
  Double-check the bucket, key, and region used by the download command.
- `Error: Unable to read the local file...`
  Confirm the local file path exists and is readable before using `S3FileUpload`.
- `Required command not found: mvn`
  Install Maven and ensure `mvn` is available on your `PATH`.

## References

- [AWS SDK for Java 2.x developer guide](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html)
- [Amazon S3 object download examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html)
- [Multipart uploads in Amazon S3 with Java](https://www.baeldung.com/aws-s3-multipart-upload)

## Contact

- Software Developer: Rod Oliveira
- GitHub: https://github.com/jrodolfo
- Webpage: https://jrodolfo.net

## License

- MIT License
- Copyright (c) 2026 Rod Oliveira
- See [LICENSE](./LICENSE)

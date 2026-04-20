# AWS S3 Java Samples

This repository is a small reference project for uploading to and downloading from Amazon S3 with Java.

The code was written over time to capture working examples, not to form a single polished application. That is still the right way to view it: a set of focused S3 samples you can read, run, and adapt.

## What Is Here

- `S3TextUpload`: upload a small text object to S3.
- `S3FileUpload`: upload a local file using multipart transfer support.
- `S3ObjectDownload`: download an object and demonstrate a few retrieval options.

All samples now use the AWS SDK for Java v2 client style and share the same runtime configuration approach.

## Project Layout

```text
Makefile
LICENSE
README.md
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

## Requirements

- Java 8 or later
- Maven 3.6 or later
- An AWS account with S3 access
- AWS credentials available locally

## Build

```bash
mvn test
```

The test suite covers runtime input parsing and sample configuration behavior. It does not exercise real S3 operations.

## Continuous Integration

GitHub Actions runs `mvn test` automatically on pushes to `main` and on pull requests. The workflow file lives at `.github/workflows/ci.yml`.

## Credentials

The samples use the standard AWS credential chain by default. A typical local setup is:

```bash
aws configure
```

`S3ObjectDownload` can also use a named profile if you pass one explicitly.

## Runtime Configuration

Each sample now follows the same input pattern:

- positional command-line arguments take priority
- if an argument is omitted, the sample checks an environment variable
- if a required value is still missing, the sample stops with a clear error message

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

## Running The Samples

You can run the samples through `make`, through the shell scripts in `scripts/`, or by calling the Java classes directly with Maven.

## Make Targets

The `Makefile` provides a compact command surface for the most common tasks:

- `make help`
- `make test`
- `make test-integration INTEGRATION_BUCKET=my-bucket`
- `make upload-text BUCKET=my-bucket KEY=hello.txt`
- `make upload-file BUCKET=my-bucket FILE=/path/to/file.zip`
- `make download-object BUCKET=my-bucket KEY=hello.txt`
- `make roundtrip-object BUCKET=my-bucket KEY=hello.txt`

Examples:

```bash
make test
make test-integration INTEGRATION_BUCKET=my-s3-sample-bucket
make upload-text BUCKET=my-s3-sample-bucket KEY=hello.txt CONTENT="hello from make"
make upload-file BUCKET=my-s3-sample-bucket FILE=/path/to/file.zip
make download-object BUCKET=my-s3-sample-bucket KEY=hello.txt
make roundtrip-object BUCKET=my-s3-sample-bucket KEY=hello.txt CONTENT="hello from make"
```

## End-To-End Example

The quickest way to try the repository end to end is the round-trip script:

```bash
make roundtrip-object BUCKET=my-s3-sample-bucket KEY=hello.txt CONTENT="hello from make"
```

This flow:

- uploads a text object
- downloads the same object immediately after
- prints the downloaded content to the console

If the bucket does not exist yet, you can create it during the upload step:

```bash
make roundtrip-object BUCKET=my-s3-sample-bucket KEY=hello.txt CONTENT="hello from make" CREATE_BUCKET=true
```

## Integration Test

The repository includes an opt-in integration test that performs a real S3 round trip:

- upload a temporary object
- download the same object
- verify the content matches
- delete the temporary object

The integration test is skipped unless `AWS_S3_INTEGRATION_BUCKET` is set.

You can run it directly with Maven:

```bash
AWS_S3_INTEGRATION_BUCKET=my-s3-sample-bucket mvn -Dtest=net.jrodolfo.awss3.integration.S3IntegrationTest test
```

Optional variables:

- `AWS_S3_INTEGRATION_REGION` with default `us-east-2`
- `AWS_S3_INTEGRATION_PROFILE` to use a named AWS profile

You can also run it through `make`:

```bash
make test-integration INTEGRATION_BUCKET=my-s3-sample-bucket
```

## Scripts

The repository includes small wrapper scripts for the three sample flows:

- `scripts/upload-text.sh`
- `scripts/upload-file.sh`
- `scripts/download-object.sh`
- `scripts/roundtrip-object.sh`

Each script validates the required arguments, sets the matching environment variables, and then runs the corresponding Java class with Maven.

Examples:

```bash
scripts/upload-text.sh my-s3-sample-bucket hello.txt "hello from shell"
scripts/upload-file.sh my-s3-sample-bucket /path/to/file.zip
scripts/download-object.sh my-s3-sample-bucket hello.txt
scripts/roundtrip-object.sh my-s3-sample-bucket hello.txt "hello from shell"
```

You can also run any script with `--help` to see its expected arguments.

## Maven Entry Points

### Single Object Upload

Arguments:

1. `bucket`
2. `key`
3. `content` (optional)
4. `region` (optional, default `us-east-2`)
5. `createBucket` (optional, default `false`)
6. `cleanup` (optional, default `false`)

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=net.jrodolfo.awss3.upload.S3TextUpload \
  -Dexec.args="my-s3-sample-bucket hello.txt 'hello from java' us-east-2 false false"
```

Notes:

- `createBucket=true` tells the sample to create the bucket before uploading.
- `cleanup=true` deletes the uploaded object and then deletes the bucket.
- If `createBucket=false`, the bucket must already exist.

### Multipart File Upload

Arguments:

1. `bucket`
2. `filePath`
3. `key` (optional, defaults to the local file name)
4. `region` (optional, default `us-east-2`)
5. `multipartThresholdBytes` (optional, default `5242880`)

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=net.jrodolfo.awss3.upload.S3FileUpload \
  -Dexec.args="my-s3-sample-bucket /path/to/file.zip file.zip us-east-2 5242880"
```

Notes:

- The bucket must already exist.
- The sample reads the local file path you provide instead of depending on an old machine-specific path.
- Files larger than the configured threshold are uploaded with S3 multipart requests.

### Download Object

Arguments:

1. `bucket`
2. `key`
3. `region` (optional, default `us-east-2`)
4. `profile` (optional)

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=net.jrodolfo.awss3.download.S3ObjectDownload \
  -Dexec.args="my-s3-sample-bucket hello.txt us-east-2 default"
```

Notes:

- If no profile is provided, the sample uses the default AWS credential chain.
- The sample prints the full object, a small byte range, and a response-header-override example.

## Why This Repository Still Matters

This project is useful as a compact S3 notebook:

- it keeps working examples close at hand
- it is small enough to understand quickly
- it gives you concrete code to copy into larger projects

It is not intended to be a production-ready S3 library.

## Next Improvements

Good next steps for the repository are:

- decide whether to keep the current sample names or rename them to match their behavior more clearly
- add a small `.env.example` or setup guide for the integration test workflow

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

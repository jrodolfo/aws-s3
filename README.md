# AWS S3 Java Samples

This repository is a small reference project for uploading to and downloading from Amazon S3 with Java.

The code was written over time to capture working examples, not to form a single polished application. That is still the right way to view it: a set of focused S3 samples you can read, run, and adapt.

## What Is Here

- `SingleS3Upload`: upload a small text object to S3.
- `MultiPartS3Upload`: upload a local file using multipart transfer support.
- `GetObject2`: download an object and demonstrate a few retrieval options.

The samples do not share one uniform AWS client style yet, but they are now configured in a consistent way at runtime.

## Project Layout

```text
src/main/java/com/example/awss3/SampleInput.java
src/main/java/com/example/awss3/upload/SingleS3Upload.java
src/main/java/com/example/awss3/upload/MultiPartS3Upload.java
src/main/java/com/example/awss3/download/GetObject2.java
src/test/java/com/example/awss3/SingleS3UploadTest.java
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

The current test suite is only a placeholder smoke test. It does not exercise real S3 behavior.

## Credentials

The samples use the standard AWS credential chain by default. A typical local setup is:

```bash
aws configure
```

`GetObject2` can also use a named profile if you pass one explicitly.

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
- `AWS_S3_MAX_THREADS`
- `AWS_S3_MULTIPART_THRESHOLD`
- `AWS_PROFILE`

## Running The Samples

You can run the classes directly with Maven.

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
  -Dexec.mainClass=com.example.awss3.upload.SingleS3Upload \
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
5. `maxThreads` (optional, default `10`)
6. `multipartThresholdBytes` (optional, default `5242880`)

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=com.example.awss3.upload.MultiPartS3Upload \
  -Dexec.args="my-s3-sample-bucket /path/to/file.zip file.zip us-east-2 10 5242880"
```

Notes:

- The bucket must already exist.
- The sample reads the local file path you provide instead of depending on an old machine-specific path.

### Download Object

Arguments:

1. `bucket`
2. `key`
3. `region` (optional, default `us-east-2`)
4. `profile` (optional)

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=com.example.awss3.download.GetObject2 \
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

- add real tests around argument handling and helper logic
- modernize and simplify the Maven dependencies
- standardize the samples around one AWS client style
- add one end-to-end sample workflow with upload and download together

## References

- [AWS SDK for Java 2.x developer guide](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html)
- [Amazon S3 object download examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html)
- [Multipart uploads in Amazon S3 with Java](https://www.baeldung.com/aws-s3-multipart-upload)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

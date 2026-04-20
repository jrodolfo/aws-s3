.PHONY: help test test-integration upload-text upload-file download-object

help:
	@printf '%s\n' \
		'Available targets:' \
		'  make test' \
		'  make test-integration INTEGRATION_BUCKET=<bucket> [INTEGRATION_REGION=<region>] [INTEGRATION_PROFILE=<profile>]' \
		'  make upload-text BUCKET=<bucket> KEY=<key> [CONTENT=<content>] [REGION=<region>] [CREATE_BUCKET=false] [CLEANUP=false]' \
		'  make upload-file BUCKET=<bucket> FILE=<file-path> [KEY=<key>] [REGION=<region>] [MAX_THREADS=10] [MULTIPART_THRESHOLD=5242880]' \
		'  make download-object BUCKET=<bucket> KEY=<key> [REGION=<region>] [PROFILE=<profile>]'

test:
	mvn test

test-integration:
	@test -n "$(INTEGRATION_BUCKET)" || (echo 'INTEGRATION_BUCKET is required' >&2; exit 1)
	@AWS_S3_INTEGRATION_BUCKET="$(INTEGRATION_BUCKET)" \
	AWS_S3_INTEGRATION_REGION="$(or $(INTEGRATION_REGION),us-east-2)" \
	AWS_S3_INTEGRATION_PROFILE="$(INTEGRATION_PROFILE)" \
	mvn -Dtest=net.jrodolfo.awss3.integration.S3IntegrationTest test

upload-text:
	@test -n "$(BUCKET)" || (echo 'BUCKET is required' >&2; exit 1)
	@test -n "$(KEY)" || (echo 'KEY is required' >&2; exit 1)
	@./scripts/upload-text.sh "$(BUCKET)" "$(KEY)" "$(or $(CONTENT),Testing with the {sdk-java})" "$(or $(REGION),us-east-2)" "$(or $(CREATE_BUCKET),false)" "$(or $(CLEANUP),false)"

upload-file:
	@test -n "$(BUCKET)" || (echo 'BUCKET is required' >&2; exit 1)
	@test -n "$(FILE)" || (echo 'FILE is required' >&2; exit 1)
	@./scripts/upload-file.sh "$(BUCKET)" "$(FILE)" "$(KEY)" "$(or $(REGION),us-east-2)" "$(or $(MAX_THREADS),10)" "$(or $(MULTIPART_THRESHOLD),5242880)"

download-object:
	@test -n "$(BUCKET)" || (echo 'BUCKET is required' >&2; exit 1)
	@test -n "$(KEY)" || (echo 'KEY is required' >&2; exit 1)
	@./scripts/download-object.sh "$(BUCKET)" "$(KEY)" "$(or $(REGION),us-east-2)" "$(PROFILE)"

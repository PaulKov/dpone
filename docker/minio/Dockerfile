# The upstream OSS image registry is no longer anonymously readable. Build the
# test-only S3 server from the immutable upstream security release instead.
FROM golang:1.24.8-bookworm@sha256:4ed690d6649d63c312b99a6120025ec79ce3b542968a37da53d6236c7c61a848 AS build
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/minio/minio@9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
COPY --from=build /out/minio /usr/local/bin/minio
RUN mkdir /data && chown 1000:1000 /data
USER 1000:1000
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]

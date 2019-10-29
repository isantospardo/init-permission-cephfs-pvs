# This Dockerfile includes a multi-stage in order to get only the go binary and do not load
# all the other files into the image.

FROM golang:1.11 AS builder
# Download and install the latest release of dep
ADD https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 /usr/bin/dep
RUN chmod +x /usr/bin/dep
# Copy the code from the host and compile it
WORKDIR $GOPATH/src/github.com/storage/init-permission-cephfs-volumes
# Use the dependency management tool for Go in to create the Gopkg.* files
# https://github.com/golang/dep
COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure --vendor-only
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o /app .

FROM scratch
COPY --from=builder /app ./
ENTRYPOINT ["./app"]

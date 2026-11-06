# object-migration: one static binary (rclone is linked as a library), so the runtime image
# only needs CA certificates. Build for the target platform, e.g.
#   docker buildx build --platform linux/amd64 -t <registry>/object-migration:<tag> .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/object-migration ./cmd/object-migration

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/object-migration /object-migration
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/object-migration"]

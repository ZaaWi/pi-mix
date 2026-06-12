FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder
ARG TARGETOS TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o ingestor .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/ingestor .
CMD ["./ingestor"]

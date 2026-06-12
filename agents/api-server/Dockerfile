FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder
ARG TARGETOS TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o api-server .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/api-server .
EXPOSE 8080
CMD ["./api-server"]

FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bridge

FROM alpine:3.21
RUN apk add --no-cache avrdude
COPY --from=builder /app/bridge /bridge
CMD ["/bridge"]

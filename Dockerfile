FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o bunkrd ./cmd/bunkrd

FROM alpine:latest
WORKDIR /app
COPY --from=build /app/bunkrd .
EXPOSE 8080
CMD ["./bunkrd", "--port=8080", "--nodes=5", "--replicas=3", "--data=/app/bunkr-data"]

FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /sandbox-engine .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates docker-cli

WORKDIR /app
COPY --from=builder /sandbox-engine /app/sandbox-engine
COPY docker /app/docker

EXPOSE 8080
CMD ["/app/sandbox-engine"]

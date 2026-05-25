FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /coordinator ./cmd/coordinator
RUN go build -o /worker ./cmd/worker

FROM alpine:3.19
RUN apk add --no-cache ca-certificates

# Both binaries in the same image — pick via CMD override in k8s
COPY --from=builder /coordinator /coordinator
COPY --from=builder /worker /worker

# Default: run the coordinator
CMD ["/coordinator"]

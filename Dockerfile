FROM golang:1.26.2-alpine3.23 AS builder

ENV CGO_ENABLED=0
ENV GOCACHE=/root/.cache/go-build

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    go build -o /tmp/broker ./cmd/main.go

# ------------------------

FROM alpine:3.23

WORKDIR /app

COPY --from=builder /tmp/broker .

CMD ["/app/broker"]
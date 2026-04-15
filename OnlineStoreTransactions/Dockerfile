FROM golang:1.26.2-alpine3.23

WORKDIR /app

COPY . .

RUN go build -o /tmp/service ./cmd/service

FROM alpine:3.23

WORKDIR /app

COPY --from=0 /tmp/service /app/service

CMD [ "/app/service" ]


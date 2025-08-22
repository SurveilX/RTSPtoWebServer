# syntax=docker/dockerfile:1

FROM --platform=${BUILDPLATFORM} golang:1.23-alpine3.21 AS builder

RUN apk add git

RUN apt-get update && apt-get install -y ffmpeg

WORKDIR /go/src/app
COPY . .

ARG TARGETOS TARGETARCH TARGETVARIANT

ENV CGO_ENABLED=0
RUN go get \
    && go mod download \
    && GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOARM=${TARGETVARIANT#"v"} go build -a -o rtsp-to-web

FROM alpine:3.21

WORKDIR /app
RUN apk add --no-cache ffmpeg


COPY --from=builder /go/src/app/rtsp-to-web /app/

RUN mkdir -p /config
COPY --from=builder /go/src/app/config.json /config

ENV GO111MODULE="on"
ENV GIN_MODE="release"

CMD ["./rtsp-to-web", "--config=/config/config.json"]

FROM golang:alpine AS builder

ENV GOPROXY https://goproxy.cn,direct
ENV CGO_ENABLED 0
ENV GOOS linux

WORKDIR /go/cache 
COPY go.mod go.sum ./
RUN go mod download
RUN apk add --no-cache make git

WORKDIR /cds
COPY . .
RUN go clean && make -f Makefile.docker build
FROM alpine as cds
WORKDIR /cds
RUN apk update --no-cache && apk add --no-cache ca-certificates tzdata
ENV TZ Asia/Shanghai


COPY --from=builder /cds/docker/build/rtu      /cds/docker/build/
COPY --from=builder /cds/docker/build/dm       /cds/docker/build/
COPY --from=builder /cds/docker/build/galaxy   /cds/docker/build/
# COPY --from=build /go/release/conf.yaml /

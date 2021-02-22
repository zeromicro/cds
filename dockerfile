FROM golang:alpine AS builder
ENV GOPROXY https://goproxy.cn,direct
ENV CGO_ENABLED 0
ENV GOOS linux
WORKDIR /go/cache 
COPY go.mod go.sum ./
RUN apk add --no-cache make git
RUN go mod download
WORKDIR /cds
COPY . .
RUN go clean && make  build


FROM alpine as cds
WORKDIR /cds
RUN apk update --no-cache && apk add --no-cache ca-certificates tzdata
ENV TZ Asia/Shanghai
COPY --from=builder /cds/build/dm       /cds/build/
COPY --from=builder /cds/build/rtu      /cds/build/
COPY --from=builder /cds/build/galaxy   /cds/build/
# COPY --from=build /go/release/conf.yaml /

FROM golang:alpine AS build

ENV GOPROXY https://goproxy.cn,direct
ENV CGO_ENABLED 0
ENV GOOS linux

WORKDIR /go/cache 
COPY go.mod go.sum ./
RUN go mod download

WORKDIR /cds
COPY . .
# RUN make build
RUN go clean && \
	GO111MODULE=on GOARCH=amd64 go build -ldflags="-s -w"  -o docker/build/rtu      rtu/cmd/sync/rtu.go && \
	GO111MODULE=on GOARCH=amd64 go build -ldflags="-s -w"  -o docker/build/dm        dm/cmd/sync/dm.go && \
	GO111MODULE=on GOARCH=amd64 go build -ldflags="-s -w"  -o docker/build/galaxy    galaxy/galaxy.go

FROM alpine as cds
WORKDIR /cds
RUN apk update --no-cache && apk add --no-cache ca-certificates tzdata
ENV TZ Asia/Shanghai


COPY --from=build /cds/docker/build/rtu      /cds/docker/build/
COPY --from=build /cds/docker/build/dm       /cds/docker/build/
COPY --from=build /cds/docker/build/galaxy   /cds/docker/build/
# COPY --from=build /go/release/conf.yaml /




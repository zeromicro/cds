FROM golang AS build
ENV GOPROXY https://goproxy.cn,direct

WORKDIR /go/cache 
COPY go.mod go.sum ./
RUN go mod download

WORKDIR /cds
COPY . .
RUN make build


FROM debian:stable-slim as cds
WORKDIR /cds
COPY --from=build /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /cds/docker/build/rtu      docker/build/
COPY --from=build /cds/docker/build/dm       docker/build/
COPY --from=build /cds/docker/build/galaxy   docker/build/
# COPY --from=build /go/release/conf.yaml /




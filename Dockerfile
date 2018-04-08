FROM golang:1.9-alpine
RUN apk add --no-cache fuse
RUN mkdir /mnt/tarfs
COPY . /go/src/github.com/cpuguy83/tarfs
WORKDIR /go/src/github.com/cpuguy83/tarfs
RUN go build -o tarfsd ./cmd/tarfsd

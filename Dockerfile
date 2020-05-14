# ----------------------------------------
# build 
#

# basic
FROM golang:latest AS builder

# build
WORKDIR /go/src/github.com/ziyht/prom_to_click
ADD . .
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo .

# ----------------------------------------
# 
#
FROM scratch AS prod

# expose /etc
VOLUME /etc/
ADD prom_to_click.yml /etc/prom_to_click.yml

# port
EXPOSE 9302

COPY --from=builder /go/src/github.com/ziyht/prom_to_click/prom_to_click .
CMD ["./prom_to_click", "--config.file=/etc/prom_to_click.yml"]

# NebulaDB server image: static binary on an empty base (no shell, no package manager).
#   docker build -t nebuladb --build-arg VERSION=0.1.0 .
FROM golang:1.27 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/naman/nebuladb/internal/server.Version=${VERSION}" -o /out/nebuladb ./cmd/nebuladb \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/nebula ./cmd/nebula \
 && mkdir /out/data

FROM scratch
COPY --from=build /out/nebuladb /out/nebula /
# The data directory must be writable by the non-root user.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
VOLUME /data
# 6380 = Redis protocol, 7380 = admin/Inspector API, 7381 = replication (leader).
EXPOSE 6380 7380 7381
# `nebula stats -addr` runs INFO against the server: non-zero exit if it is not answering.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s CMD ["/nebula", "stats", "-addr", "127.0.0.1:6380"]
ENTRYPOINT ["/nebuladb"]
# Inside a container the server must listen on all interfaces; publish the ports
# to 127.0.0.1 only (see docker-compose.yml): the admin port has no authentication.
CMD ["-dir", "/data", "-addr", "0.0.0.0:6380", "-admin-addr", "0.0.0.0:7380"]

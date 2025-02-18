
# Configuration variables
ARG RUST_VERSION=1.81
ARG FLATBUFFERS_VERSION=23.5.26
# Set BUILD_TYPE to 'jepsen' to build the jepsen-specific version of Atomix.
ARG BUILD_TYPE=release

###############################################################################
# Builder stage ###############################################################
###############################################################################


FROM rust:${RUST_VERSION} AS builder

# Install dependencies
# TODO: Maybe we also want to build protobuf from source?
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    git \
    cmake \
    make \
    clang

# Can't just install the flatbuffers packages because they are too
# old. Instead, we need to build them from source.
# Install FlatBuffers
WORKDIR /flatbuffers_build
ARG FLATBUFFERS_VERSION
RUN git clone https://github.com/google/flatbuffers.git && \
    cd flatbuffers && \
    git checkout v${FLATBUFFERS_VERSION} && \
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc) && \
    make install

# Build atomix
WORKDIR /atomix_build

# Copy the entire project
COPY . .

# Build the project
RUN cargo build --release

###############################################################################
# node-jepsen #################################################################
###############################################################################
# This is a arm64 build of jgoerzen/debian-base-minimal:bookworm pulled from Docker hub.
FROM purujit/atomix:debian-base-minimal AS debian-addons
FROM debian:bookworm AS node-jepsen

COPY --from=debian-addons /usr/local/preinit/ /usr/local/preinit/
COPY --from=debian-addons /usr/local/bin/ /usr/local/bin/
COPY --from=debian-addons /usr/local/debian-base-setup/ /usr/local/debian-base-setup/

RUN run-parts --exit-on-error --verbose /usr/local/debian-base-setup

ENV container=docker
STOPSIGNAL SIGRTMIN+3

# Basic system stuff
RUN apt-get -qy update && \
    apt-get -qy install \
        apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
        dos2unix openssh-server pwgen

# When run, boot-debian-base will call this script, which does final
# per-db-node setup stuff.
ADD setup-jepsen.sh /usr/local/preinit/03-setup-jepsen
RUN chmod +x /usr/local/preinit/03-setup-jepsen
# Add a service for systemd to run on startup. See https://medium.com/@benmorel/creating-a-linux-service-with-systemd-611b5c8b91d6
ADD atomix.service /etc/systemd/system/atomix.service
ADD config_for_jepsen.json /etc/atomix/config.json
RUN systemctl enable atomix.service

# Configure SSHD
RUN sed -i "s/#PermitRootLogin prohibit-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Enable SSH server
ENV DEBBASE_SSH=enabled

# Install Jepsen deps
RUN apt-get -qy update && \
    apt-get -qy install \
        build-essential bzip2 ca-certificates curl dirmngr dnsutils faketime iproute2 iptables iputils-ping libzip4 logrotate lsb-release man man-db netcat-openbsd net-tools ntpdate psmisc python3 rsyslog sudo tar tcpdump unzip vim wget

CMD ["/usr/local/bin/boot-debian-base"]
EXPOSE 22

###############################################################################
# node-jepsen #################################################################
###############################################################################
FROM debian:bookworm AS node-release

###############################################################################
# node ########################################################################
###############################################################################
FROM node-${BUILD_TYPE} AS node

###############################################################################
# rangeserver #################################################################
###############################################################################

FROM node AS rangeserver

# Copy the built executable from the builder stage
COPY --from=builder /atomix_build/target/release/rangeserver /usr/bin/rangeserver
COPY --from=builder /atomix_build/target/release/rangeserver /usr/bin/atomix

###############################################################################
# warden ######################################################################
###############################################################################

FROM node AS warden

# Copy the built executable from the builder stage
COPY --from=builder /atomix_build/target/release/warden /usr/bin/warden
COPY --from=builder /atomix_build/target/release/warden /usr/bin/atomix

###############################################################################
# epoch_publisher #############################################################
###############################################################################

FROM node AS epoch_publisher

# Copy the built executable from the builder stage
COPY --from=builder /atomix_build/target/release/epoch_publisher /usr/bin/epoch_publisher
COPY --from=builder /atomix_build/target/release/epoch_publisher /usr/bin/atomix

###############################################################################
# epoch_service ###############################################################
###############################################################################

FROM node AS epoch

# Copy the built executable from the builder stage
COPY --from=builder /atomix_build/target/release/epoch /usr/bin/epoch
COPY --from=builder /atomix_build/target/release/epoch /usr/bin/atomix

###############################################################################
# universe ####################################################################
###############################################################################

FROM node AS universe

# Copy the built executable from the builder stage
COPY --from=builder /atomix_build/target/release/universe /usr/bin/universe
COPY --from=builder /atomix_build/target/release/universe /usr/bin/atomix

###############################################################################
# frontend ####################################################################
###############################################################################

FROM node AS frontend

# Copy the built executable from the builder stage
COPY --from=builder /atomix_build/target/release/frontend /usr/bin/frontend
COPY --from=builder /atomix_build/target/release/frontend /usr/bin/atomix

###############################################################################
# cassandra ###################################################################
###############################################################################
FROM cassandra:5.0 AS cassandra-client
ADD schema/cassandra/atomix/keyspace.cql /etc/atomix/cassandra/keyspace.cql
ADD schema/cassandra/atomix/schema.cql /etc/atomix/cassandra/schema.cql
ADD create_schema.sh /usr/local/bin/create_schema.sh
RUN chmod +x /usr/local/bin/create_schema.sh

CMD ["/usr/local/bin/create_schema.sh"]
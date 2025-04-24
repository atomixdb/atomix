#!/bin/bash

sudo apt-get update && sudo apt-get install -y \
    git \
    cmake \
    make \
    clang


git clone https://github.com/google/flatbuffers.git && \
cd flatbuffers && \
git checkout v23.5.26 && \
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release && \
sudo make -j$(nproc) && \
sudo make install

cd ../

sudo apt-get -qy update && \
    sudo apt-get -qy install apt-transport-https

sudo apt-get -qy update && \
sudo apt-get -qy install dos2unix openssh-server pwgen

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env


# install docker
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


# install protobuf
curl -OL https://github.com/google/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip
unzip protoc-30.2-linux-x86_64.zip -d protoc3

sudo mv protoc3/bin/* /usr/local/bin/
sudo mv protoc3/include/* /usr/local/include/

# set cassandra
sudo docker run -d -p 9042:9042 --name cassandra cassandra:5.0
cd atomix
sudo docker exec -i cassandra cqlsh < schema/cassandra/atomix/keyspace.cql
sudo docker exec -i cassandra cqlsh -k atomix < schema/cassandra/atomix/schema.cql

RANGESERVER_IMG="atomix-rangeserver"
WARDEN_IMG="atomix-warden"
EPOCH_PUBLISHER_IMG="atomix-epoch-publisher"
EPOCH_IMG="atomix-epoch"
UNIVERSE_IMG="atomix-universe"
FRONTEND_IMG="atomix-frontend"
BUILD_TYPE="${BUILD_TYPE:-release}"
CASSANDRA_CLIENT_IMG="atomix-cassandra-client"

TAG="latest"

docker build -t "$RANGESERVER_IMG:$TAG" --target rangeserver --build-arg BUILD_TYPE=$BUILD_TYPE .
docker build -t "$WARDEN_IMG:$TAG" --target warden --build-arg BUILD_TYPE=$BUILD_TYPE .
docker build -t "$EPOCH_PUBLISHER_IMG:$TAG" --target epoch_publisher --build-arg BUILD_TYPE=$BUILD_TYPE .
docker build -t "$EPOCH_IMG:$TAG" --target epoch  --build-arg BUILD_TYPE=$BUILD_TYPE .
docker build -t "$UNIVERSE_IMG:$TAG" --target universe  --build-arg BUILD_TYPE=$BUILD_TYPE .
docker build -t "$FRONTEND_IMG:$TAG" --target frontend  --build-arg BUILD_TYPE=$BUILD_TYPE .
if [ "$BUILD_TYPE" = "jepsen" ]; then
    docker build -t "$CASSANDRA_CLIENT_IMG:$TAG" --target cassandra-client .
fi

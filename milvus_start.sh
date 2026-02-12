#!/usr/bin/env bash
set -e

CONTAINER_NAME=milvus-ultraragv1.1
MILVUS_IMAGE=milvusdb/milvus:v2.6.9

GRPC_PORT=29901
HTTP_PORT=29902

DATA_DIR=/home/dhj/vsworkspace/ultrarag/milvus_data

echo "==> Starting Milvus (standalone)"
echo "==> gRPC: ${GRPC_PORT}, HTTP: ${HTTP_PORT}"
echo "==> Data dir: ${DATA_DIR}"

mkdir -p ${DATA_DIR}
chown -R 1000:1000 ${DATA_DIR} 2>/dev/null || true

docker run -d \
  --name ${CONTAINER_NAME} \
  --restart unless-stopped \
  --security-opt seccomp:unconfined \
  -e DEPLOY_MODE=STANDALONE \
  -e ETCD_USE_EMBED=true \
  -e COMMON_STORAGETYPE=local \
  -v ${DATA_DIR}:/var/lib/milvus \
  -p ${GRPC_PORT}:19530 \
  -p ${HTTP_PORT}:9091 \
  --health-cmd="curl -f http://localhost:9091/healthz" \
  --health-interval=30s \
  --health-start-period=60s \
  --health-timeout=10s \
  --health-retries=3 \
  ${MILVUS_IMAGE} \
  milvus run standalone

echo "==> Waiting for Milvus to become healthy..."
sleep 5
docker ps | grep ${CONTAINER_NAME} || true
#!/bin/bash
set -e

BUCKET_NAME="hvac-data"

REGION="us-east-1"
LOCALSTACK_ENDPOINT="http://localstack:4566"

echo "⏳ Aguardando o LocalStack (serviço S3) estar completamente pronto..."
until curl -s ${LOCALSTACK_ENDPOINT}/health | grep '"s3": "running"' > /dev/null; do
  echo "LocalStack S3 não está pronto. Esperando mais 5 segundos..."
  sleep 5
done
echo "✅ LocalStack S3 está pronto!"

export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="${REGION}"
export AWS_ENDPOINT_URL="${LOCALSTACK_ENDPOINT}"


echo "🔍 Verificando se o bucket S3 '$BUCKET_NAME' já existe..."
if aws s3api head-bucket --bucket "${BUCKET_NAME}" --endpoint-url "${AWS_ENDPOINT_URL}" 2>/dev/null; then
  echo "ℹ️ Bucket '$BUCKET_NAME' já existe. Pulando a criação."
else
  echo "✨ Bucket '$BUCKET_NAME' não existe. Criando agora..."
  aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" --endpoint-url "${AWS_ENDPOINT_URL}"
  echo "🎉 Bucket '$BUCKET_NAME' criado com sucesso!"
fi

echo "✅ Script de inicialização do LocalStack concluído."

# Criar nem o script
# aws s3api create-bucket --bucket hvac-data --region us-east-1 --endpoint-url http://localhost:4566
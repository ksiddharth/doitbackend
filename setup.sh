#!/bin/bash
set -e

# Configuration
PROJECT_ID="ephemeral-access-backend"
REGION="us-central1"
QUEUE_NAME="analysis-queue"
BUCKET_NAME="${PROJECT_ID}-data"

# Tag Configuration (Update these values if needed)
# Example: TAG_VALUE="tagValues/1234567890"
# Set this env var before running if you need tagging.
TAG_VALUE="${TAG_VALUE:-}" 

echo "=== Starting Setup for ${PROJECT_ID} ==="

# 0. Apply Environment Tag (If requested)
if [ -n "$TAG_VALUE" ]; then
    echo "--> Applying Environment Tag (${TAG_VALUE})..."
    gcloud resource-manager tags bindings create \
        --tag-value="${TAG_VALUE}" \
        --parent="//cloudresourcemanager.googleapis.com/projects/${PROJECT_ID}" \
        --location=${REGION} || echo "Warning: Failed to apply tag. Continuing..."
fi

# 1. Enable APIs
echo "--> Enabling APIs (Cloud Tasks, Storage, Generative AI, Secret Manager, YouTube)..."
gcloud services enable \
    cloudtasks.googleapis.com \
    storage.googleapis.com \
    secretmanager.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    compute.googleapis.com \
    youtube.googleapis.com \
    --project=${PROJECT_ID}

echo "--> Waiting 20s for Service Account propagation..."
sleep 20

# 2. Setup Storage Lifecycle (1 Day Retention)
echo "--> Configuring Storage Lifecycle..."
# Create Bucket if not exists
if ! gcloud storage buckets describe gs://${BUCKET_NAME} --project=${PROJECT_ID} >/dev/null 2>&1; then
    echo "    Bucket not found. Creating gs://${BUCKET_NAME}..."
    gcloud storage buckets create gs://${BUCKET_NAME} --location=${REGION} --project=${PROJECT_ID}
else
    echo "    Bucket gs://${BUCKET_NAME} exists."
fi

cat > lifecycle.json <<EOF
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 1}
    }
  ]
}
EOF

gcloud storage buckets update gs://${BUCKET_NAME} --lifecycle-file=lifecycle.json --project=${PROJECT_ID}
rm lifecycle.json

# 3. Create Cloud Task Queue
echo "--> Creating Cloud Task Queue..."
if gcloud tasks queues describe ${QUEUE_NAME} --location=${REGION} --project=${PROJECT_ID} > /dev/null 2>&1; then
    echo "    Queue already exists."
else
    gcloud tasks queues create ${QUEUE_NAME} --location=${REGION} --project=${PROJECT_ID}
fi

# 4. Secrets Setup
echo "--> Setting up GEMINI_API_KEY..."
if [ -z "$GEMINI_API_KEY" ]; then
    read -s -p "Enter your Gemini API Key: " GEMINI_API_KEY
    echo ""
fi

if gcloud secrets describe GEMINI_API_KEY --project=${PROJECT_ID} > /dev/null 2>&1; then
    echo "    Secret already exists. Creating new version..."
    printf "%s" "$GEMINI_API_KEY" | gcloud secrets versions add GEMINI_API_KEY --data-file=- --project=${PROJECT_ID}
else
    printf "%s" "$GEMINI_API_KEY" | gcloud secrets create GEMINI_API_KEY --data-file=- --project=${PROJECT_ID}
fi

# 5. YouTube API Key Setup
echo "--> Setting up YOUTUBE_API_KEY..."
if [ -z "$YOUTUBE_API_KEY" ]; then
    read -s -p "Enter your YouTube Data API Key: " YOUTUBE_API_KEY
    echo ""
fi

if gcloud secrets describe YOUTUBE_API_KEY --project=${PROJECT_ID} > /dev/null 2>&1; then
    echo "    Secret already exists. Creating new version..."
    printf "%s" "$YOUTUBE_API_KEY" | gcloud secrets versions add YOUTUBE_API_KEY --data-file=- --project=${PROJECT_ID}
else
    printf "%s" "$YOUTUBE_API_KEY" | gcloud secrets create YOUTUBE_API_KEY --data-file=- --project=${PROJECT_ID}
fi

# 6. Grant Permissions
echo "--> Granting permissions..."
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud secrets add-iam-policy-binding GEMINI_API_KEY \
    --member="serviceAccount:${COMPUTE_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project=${PROJECT_ID} > /dev/null

gcloud secrets add-iam-policy-binding YOUTUBE_API_KEY \
    --member="serviceAccount:${COMPUTE_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project=${PROJECT_ID} > /dev/null

echo "=== Setup Complete! ==="


#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="airline-data-eng-project-1234"
REGION="asia-south1"
BUCKET="${PROJECT_ID}-airline-data"

echo "Checking Dataproc batches..."

gcloud dataproc batches list \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format="table(name,state)"

echo
echo "Active batches:"
gcloud dataproc batches list \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --filter="state=RUNNING OR state=PENDING" \
  --format="value(name)"

echo
echo "To cancel a batch, run:"
echo "gcloud dataproc batches cancel BATCH_ID \\"
echo "  --project=$PROJECT_ID --region=$REGION"

echo
echo "Review temporary GCS objects:"
gcloud storage ls \
  "gs://${BUCKET}/temp/" || true

echo
echo "Cleanup review completed."
echo "No resources were deleted automatically."

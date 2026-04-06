# ALWAYS: no spaces around '='
export S3_BUCKET="e2e-mlops-data-681802563986"
export TRAIN_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-train-task:2026-04-06-06-22--6610d5f@sha256:0dd8cb8701a653cfc7cc103d30f4437ec6e625d41cd57f5bc6c45223512e7acd"
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"

# sanity check (prevents silent failure)
echo "TRAIN_TASK_IMAGE=$TRAIN_TASK_IMAGE"
source .venv_train/bin/activate

python -m workflows.train.run register
python -m workflows.train.run train
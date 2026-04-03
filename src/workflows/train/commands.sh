# ALWAYS: no spaces around '='
export S3_BUCKET="e2e-mlops-data-681802563986"
export TRAIN_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-train-task:2026-04-03-08-16--475813f@sha256:3182ade927934a1f5efc98e4df457fccb76521b442fc1133e192bb03d632adba"
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"

# sanity check (prevents silent failure)
echo "TRAIN_TASK_IMAGE=$TRAIN_TASK_IMAGE"
source .venv_train/bin/activate

python -m workflows.train.run register
python -m workflows.train.run train
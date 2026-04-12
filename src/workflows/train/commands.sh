ruff check src/workflows/train --fix
export S3_BUCKET=${S3_BUCKET:-s3-temp-bucket-mlsecops-681802563986}
export TRAIN_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-train-task:2026-04-12-10-22--b18cabb@sha256:9e94cdfa439643d8d4977cd48b7703d13afa660948b64255c36037796cb7da94"
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
source .venv_train/bin/activate

python -m workflows.train.run register
python -m workflows.train.run train

# delete an execution by its id as input example afrr9jtjsj2fnnwnxm75

# (lsof -i:30081 >/dev/null 2>&1 || (kubectl -n flyte port-forward svc/flyteadmin 30081:81 >/dev/null 2>&1 & sleep 2)) && read -p "Execution ID: " id && flytectl config init --host=127.0.0.1:30081 --insecure --force >/dev/null 2>&1 && flytectl delete execution "$id" -p flytesnacks -d development && kubectl delete pod -n flytesnacks-development -l execution-id="$id"

# kubectl -n flyte port-forward svc/flyteadmin 30081:81

# flytectl get execution av7p9wzs8qg4ms4kj4vq -p flytesnacks -d development --details

source .venv_elt/bin/activate
# aws s3 ls s3://$S3_BUCKET/iceberg/warehouse --recursive && kubectl -n flyte port-forward svc/flyteadmin 30081:81
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
K8S_CLUSTER=kind python -m workflows.ELT.run elt # schedule

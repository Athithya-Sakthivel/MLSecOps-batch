# Optionally delete bucket for fresh run. make delete-temp-s3

export S3_BUCKET=$(aws sts get-caller-identity --query Account --output text | sed 's/^/s3-temp-bucket-mlsecops-/')
export MLFLOW_S3_BUCKET=$S3_BUCKET
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET
export MODEL_ARTIFACTS_S3_BUCKET=$S3_BUCKET

make temp-s3

make core                                  # create fresh kind Kubernetes cluster + default storage class

export K8S_CLUSTER=kind                    # target Kubernetes platform (kind)
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET     # S3 bucket storing Postgres backups
export PG_CLUSTER_ID=cnpg-cluster-kind     # stable S3 namespace for this environment
export PG_SERVER_NAME=mlsecops             # stable backup lineage identifier
make pg-cluster                            # deploy fresh Postgres cluster (no restore, no initial backup)

make elt                                   # deploy Iceberg + Spark + Flyte and run ELT pipeline

make prune-elt                             # cleanup Spark operator / ELT-related resources

export K8S_CLUSTER=kind                    # re-export (ensures env consistency)
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET   # same S3 bucket (must match lineage)
export PG_CLUSTER_ID=cnpg-cluster-kind     # same namespace (must NOT change)
export PG_SERVER_NAME=mlsecops             # same lineage (must NOT change)
make pg-backup                             # create base backup + archive WAL to S3

make core                                  # destroy and recreate Kubernetes cluster (stateless reset)

export K8S_CLUSTER=kind                    # reconfigure environment after cluster reset
export PG_BACKUPS_S3_BUCKET=$S3_BUCKET   # same backup bucket
export PG_CLUSTER_ID=cnpg-cluster-kind     # same backup namespace
export PG_SERVER_NAME=mlsecops             # same lineage name

# Restore iceberg tables so train workflow can read persisted data (Iceberg metadata lives in Postgres)
make pg-restore-latest                     # restore latest base backup + WAL from s3 into fresh k8s cluster

make train                                 # run Flyte training workflow (consumes Gold Iceberg tables)

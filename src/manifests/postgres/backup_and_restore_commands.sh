#!/usr/bin/env bash
export K8S_CLUSTER=kind
export TARGET_NS=default
export PG_BACKUPS_S3_BUCKET=e2e-mlops-data-681802563986
export PG_CLUSTER_ID=cnpg-cluster-kind
export BACKUP_PREFIX=postgres_backups/
export BACKUP_DESTINATION_PATH=s3://e2e-mlops-data-681802563986/postgres_backups/cnpg-cluster-kind/
export BACKUP_ENDPOINT_URL=''
export BACKUP_RETENTION_POLICY=30d
export BACKUP_SCHEDULE=0\ 0\ 0\ \*\ \*\ \*
export PG_SERVER_NAME=backup
export RESTORE_SOURCE_SERVER_NAME=backup
export RESTORE_SERVER_NAME=mlsecops
export CREATE_INITIAL_BACKUP=true
export IRSA_ROLE_ARN=''
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN

MODE="${MODE:-backup}"
TARGET_TIME="${TARGET_TIME:-mlsecops}"

case "$MODE" in
  backup)
    echo "Backup current lineage"
K8S_CLUSTER=kind CREATE_INITIAL_BACKUP=true PG_BACKUPS_S3_BUCKET=e2e-mlops-data-681802563986 PG_CLUSTER_ID=cnpg-cluster-kind PG_SERVER_NAME=backup bash src/infra/core/postgres_cluster.sh backup --wait
    ;;

  restore|restore-latest)
    echo "Restore latest into a new unique lineage"
K8S_CLUSTER=kind PG_BACKUPS_S3_BUCKET=e2e-mlops-data-681802563986 PG_CLUSTER_ID=cnpg-cluster-kind RESTORE_SOURCE_SERVER_NAME=backup RESTORE_SERVER_NAME=mlsecops bash src/infra/core/postgres_cluster.sh deploy --restore latest --force-recreate
    ;;

  restore-time)
    if [[ -z "$TARGET_TIME" ]]; then
      echo "ERROR: TARGET_TIME must be set for MODE=restore-time"
      exit 1
    fi
    echo "Restore point-in-time into a new unique lineage"
K8S_CLUSTER=kind PG_BACKUPS_S3_BUCKET=e2e-mlops-data-681802563986 PG_CLUSTER_ID=cnpg-cluster-kind RESTORE_SOURCE_SERVER_NAME=backup RESTORE_SERVER_NAME=mlsecops bash src/infra/core/postgres_cluster.sh deploy --restore time --target-time mlsecops --force-recreate
    ;;

  *)
    echo "ERROR: invalid MODE=$MODE"
    echo "Valid values: backup | restore | restore-latest | restore-time"
    exit 1
    ;;
esac

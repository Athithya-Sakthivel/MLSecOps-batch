core:
	kind delete cluster --name local-cluster || true && kind create cluster --name local-cluster && \
	bash src/infra/core/default_storage_class.sh

POSTGRES_SH := bash src/infra/core/postgres_cluster.sh

.PHONY: pg-cluster pg-backup pg-restore-latest pg-restore-time

pg-cluster:
	$(POSTGRES_SH) deploy --create-initial-backup false

pg-backup:
	$(POSTGRES_SH) backup --wait

pg-restore-latest:
	$(POSTGRES_SH) deploy --restore latest --force-recreate

pg-restore-time:
	@test -n "$$TARGET_TIME" || (echo "ERROR: TARGET_TIME must be set (RFC3339)" && exit 1)
	$(POSTGRES_SH) deploy --restore time --target-time "$$TARGET_TIME" --force-recreate

elt:
	bash src/infra/elt/iceberg.sh --rollout && bash src/infra/elt/spark_operator.sh --rollout && \
	python3 src/infra/core/flyte_setup.py --rollout && bash src/workflows/ELT/run.sh && echo "sleep for 1500 seconds..." && sleep 1500 && kubectl get pods -A

prune-elt:
	bash src/infra/elt/spark_operator.sh --cleanup
	
train:
	bash src/infra/elt/iceberg.sh --rollout && \
	python3 src/infra/core/flyte_setup.py --rollout && \
	python3 src/infra/train/mlflow_server.py --rollout && \
	bash src/workflows/train/commands.sh && echo "sleep for 600 seconds..."

prune-train:
	python3 src/infra/train/mlflow_server.py --delete && python3 src/infra/core/flyte_setup.py --delete

temp-s3:
	ACCOUNT_ID=$$(aws sts get-caller-identity --query Account --output text); \
	BUCKET=s3-temp-bucket-mlsecops-$$ACCOUNT_ID; \
	REGION=$$AWS_REGION; \
	if ! aws s3api head-bucket --bucket $$BUCKET 2>/dev/null; then \
		aws s3api create-bucket \
			--bucket $$BUCKET \
			--region $$REGION \
			--create-bucket-configuration LocationConstraint=$$REGION; \
		echo "Created $$BUCKET"; \
	else \
		echo "Bucket $$BUCKET already exists"; \
	fi

delete-temp-s3:
	ACCOUNT_ID=$$(aws sts get-caller-identity --query Account --output text); \
	BUCKET=s3-temp-bucket-mlsecops-$$ACCOUNT_ID; \
	REGION=$$AWS_REGION; \
	if aws s3api head-bucket --bucket $$BUCKET 2>/dev/null; then \
		aws s3 rm s3://$$BUCKET --recursive; \
		aws s3api delete-bucket --bucket $$BUCKET --region $$REGION; \
		echo "Deleted $$BUCKET"; \
	else \
		echo "Bucket $$BUCKET does not exist"; \
	fi

tree:
	tree -a -I '.git|.venv|archive|__pycache__|.venv_deploy|.venv_elt|.venv_train|.ruff_cache'

push:
	git add .
	git commit -m "new"
	gitleaks detect --source . --exit-code 1 --redact
	git push origin main --force

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.log" ! -path "./.git/*" -delete
	find . -type f -name "*.pulumi-logs" ! -path "./.git/*" -delete
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf logs
	rm -rf src/terraform/.plans
	clear

recreate:
	make rollout-pg && bash src/tests/core/postgres_cluster.sh || true

rollout-signoz:
	bash src/core/signoz.sh --rollout && bash src/tests/signoz.sh


validate-pg:
	kind delete cluster --name local-cluster || true && kind create cluster --name local-cluster && \
	K8S_CLUSTER=kind bash src/infra/core/default_storage_class.sh && \
	K8S_CLUSTER=kind bash src/infra/core/postgres_cluster.sh deploy && \
	bash src/tests/infra/validate_cnpg_latest_restore.sh && \
	bash src/tests/infra/validate_cnpg_PITR.sh && \
	aws s3 ls s3://$$PG_BACKUPS_S3_BUCKET/postgres_backups/ --recursive



iac-staging:
	bash src/terraform/run.sh --create --env staging || true
delete-iac-staging:
	bash src/terraform/run.sh --delete --yes-delete --env staging

test-iac-staging:
	bash src/terraform/run.sh --create --env staging || true && \
	bash src/terraform/run.sh --delete --yes-delete --env staging

sync:
	aws s3 sync s3://$$S3_BUCKET/iceberg/warehouse/ $(pwd)/data/iceberg/

set-staging-eks-context:
	./src/scripts/set_k8s_context.sh staging

set-prod-eks-context:
	./src/scripts/set_k8s_context.sh prod

set-kind-context:
	kubectl config use-context kind-rag8s-local


delete-cloudflared-agents:
	python3 infra/generators/cloudflared.py --delete --namespace inference || true

cloudflare-setup:
	bash infra/setup/cloudflared.sh

cloudflare-logout:
	rm -rf ~/.cloudflared && \
	rm -f ~/.config/rag/secrets.env && \
	unset CLOUDFLARE_TUNNEL_TOKEN && \
	unset CLOUDFLARE_TUNNEL_CREDENTIALS_B64 && \
	unset CLOUDFLARE_TUNNEL_NAME




 
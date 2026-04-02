
#!/usr/bin/env bash
set -euo pipefail

source .venv_train/bin/activate

export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"

: "${TRAIN_TASK_IMAGE:?set TRAIN_TASK_IMAGE to the built train task image before registering}"

python -m workflows.train.run register
python -m workflows.train.run train
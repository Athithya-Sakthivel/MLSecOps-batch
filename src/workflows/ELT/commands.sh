export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-30-07-33--15e04f8@sha256:d7b6aaab07888a8f4b5dcff26dab6c769352a67c4a02b5b12a66e28ccba0b4"
source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.ELT.run register
python -m workflows.ELT.run elt

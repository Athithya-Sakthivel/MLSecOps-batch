source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.ELT.run register
python -m workflows.ELT.run elt





docker pull ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-31-19-56--2db12a9



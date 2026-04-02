source .venv_train/bin/activate
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.train.run register
python -m workflows.train.run train
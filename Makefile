
default: venv/bin/poetry
	. venv/bin/activate && poetry install --sync

venv:
	python3.10 -m venv ./venv  # or use pyenv or poetry
	. venv/bin/activate && pip install --upgrade pip

venv/bin/poetry: venv
	. venv/bin/activate && pip install poetry


FROM python:3.10-slim

ENV PIP_ROOT_USER_ACTION=ignore \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .

# WICHTIG: pip upgraden + trusted-hosts, dann requirements installieren
RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install \
       --no-cache-dir \
       --trusted-host pypi.org \
       --trusted-host files.pythonhosted.org \
       -r requirements.txt

COPY app.py .

CMD ["python", "app.py"]


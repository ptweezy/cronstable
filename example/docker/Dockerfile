FROM python:3.14-slim@sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170

RUN pip install --no-cache-dir cronstable

COPY cronstable.yaml /etc/cronstable.d/cronstable.yaml

ENTRYPOINT ["cronstable"]

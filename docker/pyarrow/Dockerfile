FROM python:3.13.1-slim

COPY docker/pyarrow/requirements.pyarrow.txt .
RUN pip install --no-cache-dir -r requirements.pyarrow.txt

COPY src/ /src/
COPY settings/ /settings/
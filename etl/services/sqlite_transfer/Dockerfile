FROM python:3.10

WORKDIR /opt/sqlite_transfer

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

CMD ["python3", "load_data.py"]

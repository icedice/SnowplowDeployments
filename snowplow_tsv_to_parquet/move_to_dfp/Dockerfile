FROM python:3.6

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY src/* ./

ENTRYPOINT ["python3", "-u", "move_to_dfp.py"]

FROM python:3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .

CMD [ "python", "./main.py" ]
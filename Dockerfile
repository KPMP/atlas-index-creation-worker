FROM python:3.8.6-alpine3.12
WORKDIR /project
ADD . /project
RUN apk add --no-cache tzdata
RUN pip install -r requirements.txt
CMD ["python", "application.py"]

FROM python:3.11.0a3-alpine3.15
WORKDIR /project
ADD . /project
RUN apk add --no-cache tzdata
RUN pip install -r requirements.txt
CMD ["python", "application.py"]

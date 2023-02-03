FROM nickgryg/alpine-pandas:latest
WORKDIR /project
ADD . /project
RUN apk add --no-cache tzdata
RUN pip install -r requirements.txt
CMD ["python", "application.py"]
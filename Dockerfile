FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y python3 python3-pip
RUN apt-get install gcc git make -y

COPY scheduler/ scheduler
COPY resources/requirements.txt requirements.txt

RUN python3 -m pip install git+https://github.com/benoitc/http-parser
RUN python3 -m pip install -r requirements.txt

ENTRYPOINT ["python", "scheduler/main.py"]

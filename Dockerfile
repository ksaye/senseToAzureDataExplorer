FROM ubuntu:18.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends tzdata python3-pip

RUN pip3 install --upgrade pip
RUN pip3 install setuptools
RUN pip3 install azure-kusto-data azure-kusto-ingest sense-energy pandas

WORKDIR /sense

RUN ln -fs /usr/share/zoneinfo/America/Chicago /etc/localtime && dpkg-reconfigure --frontend noninteractive tzdata

COPY . .

CMD [ "python3", "-u", "./sense.py" ]

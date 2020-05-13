FROM gcr.io/gcp-runtimes/ubuntu_18_0_4:latest as base
# FROM marketplace.gcr.io/google/ubuntu1804:latest as base
# FROM marketplace.gcr.io/google/debian9:latest as base
# FROM python:3.8-slim as base
LABEL maintainer="aronchick"

FROM base as builder

RUN apt-get -y update && apt-get -y install python3-all python3-pip

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt

ARG FIRSTCACHEBUST=1

RUN python3 -m pip install -U pip
RUN python3 -m pip install --no-cache-dir -r /requirements.txt

# RUN python3 -m pip install --no-cache-dir --prefix='/install' -r /requirements.txt

# FROM base

# COPY --from=builder /install /usr/local
# COPY /install /usr/local

ARG SECONDCACHEBUST=1

RUN mkdir /src
WORKDIR /src

COPY .parameters/ .parameters

COPY ./src/. /src/

ENTRYPOINT ["./entrypoint.sh"]

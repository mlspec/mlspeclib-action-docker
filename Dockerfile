FROM marketplace.gcr.io/google/ubuntu1804:latest

LABEL maintainer="aronchick"

COPY /code /code
ENTRYPOINT ["/code/entrypoint.sh"]

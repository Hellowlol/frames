FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8

LABEL maintainer="hellowlol"

RUN pip install requirements.txt

# For manual install.
#WORKDIR /app/bw_plex

COPY ./frames /app

RUN pip3 install -e .

# COPY root/ /
VOLUME /config

#
CMD ["python", "/app/frames/fake.py --du ${url} -df ${default_folder}"]



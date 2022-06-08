# syntax = docker/dockerfile:1.0-experimental
FROM python:3.9-rc-slim-buster


# create a folder and cd into it
RUN mkdir neptune-trading-engine
RUN cd neptune-trading-engine

# set folder as current working directory
WORKDIR /neptune-trading-engine
RUN mkdir exchange_feeds

# copy files from current system to docker directory
COPY exchange_feeds exchange_feeds
COPY requirements.txt .

RUN pip install -r requirements.txt

ENV NEPTUNETRADINGENGINEPATH=/neptune-trading-engine
ENV PYTHONPATH ${NEPTUNETRADINGENGINEPATH}

CMD ["sh","-c", "python3 exchange_feeds/subscribe_feed.py -t $SYMBOL -n $STREAM_NAME"]

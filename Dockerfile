# syntax = docker/dockerfile:1.0-experimental
FROM python:3.9


# create a folder and cd into it
RUN mkdir neptune-trading-engine
RUN cd neptune-trading-engine

# set folder as current working directory
WORKDIR /neptune-trading-engine

# copy files from current system to docker directory
COPY . .

RUN pip install -r requirements.txt

ENV NEPTUNETRADINGENGINEPATH=/neptune-trading-engine
ENV PYTHONPATH ${NEPTUNETRADINGENGINEPATH}
CMD ["sh","-c", "python3 subscribe_feed.py -t $SYMBOL -n $STREAM_NAME"]

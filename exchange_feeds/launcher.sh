#!/bin/bash

WORKDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $WORKDIR

source /home/ubuntu/phobos/env/bin/activate
source $HOME/.profile

python3 feed_ingestor_to_postgres.py

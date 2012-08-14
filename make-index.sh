#!/bin/bash
python ./tar_index.py $1 | gzip > $2


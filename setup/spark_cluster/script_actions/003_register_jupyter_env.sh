#!/usr/bin/env bash

sudo sed -i '/python3_executable_path/c\ \"python3_executable_path\" : \"/usr/bin/anaconda/envs/spark-crawl/bin/python3\"' /home/spark/.sparkmagic/config.json
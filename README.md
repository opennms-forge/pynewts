# pynewts

## Overview

This project includes some sample code in Python which can be used to query samples stored by [Newts](https://github.com/OpenNMS/newts) directly in Cassandra.

## Usage

Setup a virtualenv with the requirements:
```
virtualenv -p python3.6 venv
source venv/bin/activate
pip install -r requirements.txt
```

Edit the constants in `newts.py` to point to your Cassandra cluster, and query the desired resource.

Query:
```
python newts.py
```

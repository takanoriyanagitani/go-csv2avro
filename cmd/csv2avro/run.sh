#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

cat ./sample.d/sample.csv |
	./csv2avro |
	rq \
		--input-avro \
		--output-json |
	jaq --compact-output

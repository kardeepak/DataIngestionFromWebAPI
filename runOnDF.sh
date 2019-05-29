#!/bin/bash

RUNNER=DataflowRunner
REGION=asia-east1
ZONE=asia-south1-c
PROJECT=tsl-datalake
NETWORK=vpc-tsl
SUBNET="regions/asia-south1/subnetworks/subnet-1"
TEMP_LOCATION="gs://tsl_datalake/tmp/"
STAGING_LOCATION="gs://tsl_datalake/tmp/"
INPUT_SUBSCRIPTION="projects/tsl-datalake/subscriptions/dataingestion"
JOB_NAME="webapi-data-ingestion"

python main.py --input_subscription $INPUT_SUBSCRIPTION \
--setup_file ./setup.py \
--extra_package dist/etl-1.1.1.tar.gz \
--job_name $JOB_NAME \
--runner $RUNNER \
--project $PROJECT \
--region $REGION \
--zone $ZONE \
--network $NETWORK \
--subnetwork $SUBNET \
--temp_location $TEMP_LOCATION \
--staging_location $STAGING_LOCATION \
--streaming

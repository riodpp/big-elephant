#!/bin/bash

docker compose up -d
docker network connect airflow-docker_default postgres-container-airflow
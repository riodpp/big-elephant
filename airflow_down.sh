#!/bin/bash

docker network disconnect airflow-docker_default postgres-container-airflow
docker compose down
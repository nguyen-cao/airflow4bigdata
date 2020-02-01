# init:
# 	docker pull puckel/docker-airflow

# start_webserver:
# 	docker run --rm --name docker-airflow-webserver -d -p 8080:8080 puckel/docker-airflow webserver

# stop_webserver:
# 	docker stop docker-airflow-webserver

# setup_db:
# 	docker exec docker-airflow-webserver airflow initdb

# start_scheduler:
# 	docker run --rm --name docker-airflow-scheduler -d puckel/docker-airflow scheduler

start_airflow:
	docker-compose -f docker-compose.yml -p airflow up -d

stop_airflow:
	docker-compose -f docker-compose.yml -p airflow down

start_postgres:
	docker-compose -f docker-compose-postgres.yml -p postgres up -d

stop_postgres:
	docker-compose -f docker-compose-postgres.yml -p postgres down	

start_spark:
	docker-compose -f docker-compose-spark.yml -p spark up -d

stop_spark:
	docker-compose -f docker-compose-spark.yml -p spark down
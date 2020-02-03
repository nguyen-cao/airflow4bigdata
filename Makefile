start:
	docker-compose -f docker-compose.yml up -d

stop:
	docker-compose -f docker-compose.yml down

start_airflow:
	docker-compose -f docker-compose.yml up -d airflow_redis airflow_postgres airflow_webserver flower airflow_scheduler airflow_worker

stop_airflow:
	docker-compose -f docker-compose.yml stop airflow_redis airflow_postgres airflow_webserver flower airflow_scheduler airflow_worker

start_postgres:
	docker-compose -f docker-compose.yml up -d postgres

stop_postgres:
	docker-compose -f docker-compose.yml stop postgres	

start_spark:
	docker-compose -f docker-compose.yml up -d spark-master spark-worker-1 spark-worker-2

stop_spark:
	docker-compose -f docker-compose.yml stop spark-master spark-worker-1 spark-worker-2
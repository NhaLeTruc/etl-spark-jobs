# Docker Compose wrapper
DC = docker compose

.PHONY: up down deep_down deep_clean apps_zip spark_submit run_scaled

up:
	sudo $(DC) up -d

down:
	sudo $(DC) down

deep_down:
	sudo $(DC) down --volumes --remove-orphans
	sudo docker network prune -f

deep_clean:
	sudo $(DC) down --volumes --remove-orphans
	sudo docker network prune -f
	sudo docker builder prune -fa

apps_zip:
	zip -r apps.zip apps/*
	zip -r core.zip apps/core/*
	zip -r pipelines.zip apps/pipelines/*
	mv apps.zip core.zip pipelines.zip spark/apps/
	cp apps/pipelines/main.py spark/apps/

spark_submit:
	sudo docker exec -w /opt/bitnami/spark/apps spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--py-files apps.zip \
		main.py

test_pipelines:
	make up
	make apps_zip
	make spark_submit

code_quality:
	ruff check --fix apps
	mypy --pretty apps

run_scaled:
	make down && docker-compose up --scale spark-worker=3

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
	zip -r apps.zip apps/
	mv apps.zip spark/apps/
	cp apps/pipelines/main.py spark/apps/

spark_submit:
	sudo docker exec -w /opt/bitnami/spark/apps spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--py-files apps.zip \
		main.py

test_pipelines:
	make deploy_dist
	make spark_submit

code_quality:
	ruff check --fix apps
	mypy --pretty apps

build:
	python3 -m build

deploy_dist:
	cp dist/*.whl spark/apps/artifact.whl
	cp apps/pipelines/main.py spark/apps/

run_scaled:
	make down && docker-compose up --scale spark-worker=3

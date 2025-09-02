# Docker Compose wrapper
DC = docker compose

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
	mv apps.zip spark/apps/
	cp apps/pipelines/ingest_transactions/main.py spark/apps/

spark_submit:
	sudo docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--py-files apps.zip \
		main.py

run-scaled:
	make down && docker-compose up --scale spark-worker=3

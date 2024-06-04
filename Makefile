all-infra:
	make postgres
	make superset
down-infra:
	docker-compose -f ./local_env/postgres/docker-compose.yml down
	docker-compose -f ./local_env/superset/docker-compose.yml down
postgres:
	docker-compose -f ./local_env/postgres/docker-compose.yml up -d
superset:
	docker-compose -f ./local_env/superset/docker-compose.yml up -d
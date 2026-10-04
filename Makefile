# Run from the course root.
# Docker Compose (both services, their databases and Kafka):
#   make up                              # build and start; waits until every service is healthy
#   make logs                            # follow service-order and service-inventory
#   make demo-circuit-breaker            # Inventory down -> circuit OPEN -> recovered
#   make down                            # stop and remove containers (volumes stay)
# Local Kubernetes (infra + Inventory + Order in namespace geovannycode):
#   make k8s-up                          # minikube (default)
#   make k8s-up CLUSTER=docker-desktop   # Kubernetes built into Docker Desktop
#   make k8s-validate k8s-lint           # server-side dry run, kubeconform and kube-linter
CLUSTER    ?= minikube
NAMESPACE  := geovannycode
INVENTORY_IMAGE := geovannycode/service-inventory:0.0.1-SNAPSHOT
ORDER_IMAGE     := geovannycode/service-order:0.0.1-SNAPSHOT
KUBECTL    := kubectl --context $(CLUSTER)
MYSQL_ENV     := k8s/infra/mysql/.env
POSTGRES_ENV  := k8s/infra/postgres/.env
INVENTORY_ENV := k8s/inventory/overlays/minikube/.env
ORDER_ENV     := k8s/order/overlays/minikube/.env
KUBECONFORM := ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e
KUBE_LINTER := stackrox/kube-linter:v0.8.3@sha256:f2bfce7879206d32f69ab6572c376f916643f54ca291ac38cf7d01ef591ff3f9

.PHONY: up down logs demo-circuit-breaker

# The root .env (MySQL) and order-service/.env (PostgreSQL) share DB_* names, and Compose gives shell variables
# precedence over .env: a terminal that sourced order-service/.env would recreate MySQL with Order's port,
# database and user. Dropping them here makes Compose read the root .env as intended.
COMPOSE := env -u DB_HOST -u DB_PORT -u DB_NAME -u DB_USER -u DB_PASSWORD docker compose

up: .env
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f service-order service-inventory

demo-circuit-breaker:
	./scripts/demo-circuit-breaker.sh

.PHONY: k8s-up k8s-cluster k8s-secrets k8s-images k8s-namespace k8s-infra k8s-migrate k8s-apps k8s-validate k8s-lint k8s-url k8s-load k8s-down k8s-purge

# Order of a fresh deploy: infra (databases, Kafka) -> topic -> Order's migration Job -> both services.
k8s-up: k8s-secrets k8s-cluster k8s-images k8s-namespace k8s-infra k8s-migrate k8s-apps
	$(MAKE) k8s-url

k8s-cluster:
ifeq ($(CLUSTER),minikube)
	minikube status >/dev/null 2>&1 || minikube start --cpus=4 --memory=6g
	minikube addons enable metrics-server
else
	$(KUBECTL) apply -k k8s/addons/metrics-server
	$(KUBECTL) rollout status deployment/metrics-server -n kube-system --timeout=120s
endif

# Derives the git-ignored secret files from the root .env (docker compose uses the same values).
# File targets: they are regenerated whenever the root .env is newer, so a password change reaches K8s.
k8s-secrets: $(MYSQL_ENV) $(POSTGRES_ENV) $(INVENTORY_ENV) $(ORDER_ENV)

.env:
	@echo "Falta .env en la raíz: cp .env.example .env y completa las contraseñas"; exit 1

$(MYSQL_ENV): .env
	@umask 077; set -a; . ./.env; set +a; \
		printf 'MYSQL_ROOT_PASSWORD=%s\nMYSQL_DATABASE=%s\nMYSQL_USER=%s\nMYSQL_PASSWORD=%s\n' \
		"$$MYSQL_ROOT_PASSWORD" "$${DB_NAME:-inventory}" "$${DB_USER:-inventory}" "$$DB_PASSWORD" > $@
	@echo "Generado $@ (MySQL solo aplica MYSQL_PASSWORD con el volumen vacío; si ya hay datos, cámbiala con ALTER USER)"

$(INVENTORY_ENV): .env
	@umask 077; set -a; . ./.env; set +a; \
		printf 'DB_USER=%s\nDB_PASSWORD=%s\n' "$${DB_USER:-inventory}" "$$DB_PASSWORD" > $@
	@echo "Generado $@"

$(POSTGRES_ENV): .env
	@umask 077; set -a; . ./.env; set +a; \
		printf 'POSTGRES_DB=%s\nPOSTGRES_USER=%s\nPOSTGRES_PASSWORD=%s\n' \
		"$${POSTGRES_DB:-orderdb}" "$${POSTGRES_USER:-order}" "$$POSTGRES_PASSWORD" > $@
	@echo "Generado $@ (PostgreSQL solo aplica POSTGRES_PASSWORD con el volumen vacío)"

$(ORDER_ENV): .env
	@umask 077; set -a; . ./.env; set +a; \
		printf 'DB_USER=%s\nDB_PASSWORD=%s\n' "$${POSTGRES_USER:-order}" "$$POSTGRES_PASSWORD" > $@
	@echo "Generado $@"

# imagePullPolicy is Never: the images must already be in the node's image store.
k8s-images:
ifeq ($(CLUSTER),minikube)
	eval $$(minikube docker-env) && \
		docker build --build-context contracts=contracts -t $(INVENTORY_IMAGE) inventory-service && \
		docker build --build-context contracts=contracts -t $(ORDER_IMAGE) order-service
else
	docker build --build-context contracts=contracts -t $(INVENTORY_IMAGE) inventory-service
	docker build --build-context contracts=contracts -t $(ORDER_IMAGE) order-service
endif

# Kept out of every other kustomization so deleting the apps never deletes the namespace (and the databases).
k8s-namespace:
	$(KUBECTL) apply -k k8s/namespace

# Jobs are immutable: the previous run is deleted so the apply can recreate it (both Jobs are idempotent).
k8s-infra:
	$(KUBECTL) apply -k k8s/infra/mysql
	$(KUBECTL) apply -k k8s/infra/postgres
	$(KUBECTL) delete job kafka-topics -n $(NAMESPACE) --ignore-not-found
	$(KUBECTL) apply -k k8s/infra/kafka
	$(KUBECTL) rollout status statefulset/mysql -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) rollout status statefulset/postgres -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) rollout status statefulset/kafka -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) wait --for=condition=complete job/kafka-topics -n $(NAMESPACE) --timeout=180s

# Liquibase before the rollout (ADR 0006). "component notin (api)" applies Order's overlay without its
# Deployment: the Job plus the ConfigMap, Secret and ServiceAccount it needs.
k8s-migrate:
	$(KUBECTL) delete job service-order-db-migration -n $(NAMESPACE) --ignore-not-found
	$(KUBECTL) apply -k k8s/order/overlays/minikube -l 'app.kubernetes.io/component notin (api)'
	$(KUBECTL) wait --for=condition=complete job/service-order-db-migration -n $(NAMESPACE) --timeout=300s
	$(KUBECTL) logs job/service-order-db-migration -n $(NAMESPACE) | grep -i liquibase | tail -3

k8s-apps:
	$(KUBECTL) apply -k k8s/inventory/overlays/minikube
	$(KUBECTL) apply -k k8s/order/overlays/minikube
	@# Same tag + imagePullPolicy Never: an unchanged Deployment would keep running the previous build.
	$(KUBECTL) rollout restart deployment/service-inventory deployment/service-order -n $(NAMESPACE)
	$(KUBECTL) rollout status deployment/service-inventory -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) rollout status deployment/service-order -n $(NAMESPACE) --timeout=180s

k8s-validate: k8s-secrets k8s-namespace
	$(KUBECTL) apply -k k8s/overlays/minikube --dry-run=server

# Schema validation (kubeconform) and best-practice checks (kube-linter) of everything make k8s-up deploys.
k8s-lint: k8s-secrets
	kubectl kustomize k8s/overlays/minikube | docker run --rm -i $(KUBECONFORM) -strict -summary -
	kubectl kustomize k8s/overlays/minikube | docker run --rm -i $(KUBE_LINTER) lint -

# minikube (Docker driver on macOS/Windows) opens a tunnel per service and blocks: run each in its own terminal.
k8s-url:
ifeq ($(CLUSTER),minikube)
	@echo "minikube service service-inventory -n $(NAMESPACE) --url   # domainISk8s"
	@echo "minikube service service-order -n $(NAMESPACE) --url       # domainOSk8s"
else
	@echo "domainISk8s = http://localhost:$$($(KUBECTL) get service service-inventory -n $(NAMESPACE) -o jsonpath='{.spec.ports[0].nodePort}')"
	@echo "domainOSk8s = http://localhost:$$($(KUBECTL) get service service-order -n $(NAMESPACE) -o jsonpath='{.spec.ports[0].nodePort}')"
endif

# In-cluster load against GET /inventories; watch it with: kubectl get hpa -n geovannycode -w
k8s-load:
	$(KUBECTL) run k6-load -n $(NAMESPACE) --rm -i --restart=Never --image=grafana/k6:2.3.0@sha256:9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34 -- \
		run --vus 50 --duration 3m - < k8s/load/inventories.js

# Removes the services only; databases, Kafka and their data stay.
k8s-down:
	$(KUBECTL) delete -k k8s/order/overlays/minikube --ignore-not-found
	$(KUBECTL) delete -k k8s/inventory/overlays/minikube --ignore-not-found

# Destroys everything in the namespace, including the MySQL, PostgreSQL and Kafka volumes.
k8s-purge:
	$(KUBECTL) delete namespace $(NAMESPACE) --ignore-not-found

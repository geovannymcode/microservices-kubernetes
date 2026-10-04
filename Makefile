# Local Kubernetes workflow for service-inventory. Run from the course root.
#   make k8s-up                          # minikube (default)
#   make k8s-up CLUSTER=docker-desktop   # Kubernetes built into Docker Desktop
CLUSTER    ?= minikube
NAMESPACE  := geovannycode
IMAGE      := geovannycode/service-inventory:0.0.1-SNAPSHOT
KUBECTL    := kubectl --context $(CLUSTER)
MYSQL_ENV  := k8s/mysql/.env
APP_ENV    := k8s/overlays/minikube/.env

.PHONY: k8s-up k8s-cluster k8s-secrets k8s-image k8s-namespace k8s-validate k8s-url k8s-load k8s-down k8s-purge

k8s-up: k8s-secrets k8s-cluster k8s-image k8s-namespace
	$(KUBECTL) apply -k k8s/mysql
	$(KUBECTL) rollout status statefulset/mysql -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) apply -k k8s/overlays/minikube
	@# Same tag + imagePullPolicy Never: an unchanged Deployment would keep running the previous build.
	$(KUBECTL) rollout restart deployment/service-inventory -n $(NAMESPACE)
	$(KUBECTL) rollout status deployment/service-inventory -n $(NAMESPACE) --timeout=180s
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
k8s-secrets: $(MYSQL_ENV) $(APP_ENV)

.env:
	@echo "Falta .env en la raíz: cp .env.example .env y completa las contraseñas"; exit 1

$(MYSQL_ENV): .env
	@umask 077; set -a; . ./.env; set +a; \
		printf 'MYSQL_ROOT_PASSWORD=%s\nMYSQL_DATABASE=%s\nMYSQL_USER=%s\nMYSQL_PASSWORD=%s\n' \
		"$$MYSQL_ROOT_PASSWORD" "$${DB_NAME:-inventory}" "$${DB_USER:-inventory}" "$$DB_PASSWORD" > $@
	@echo "Generado $@ (MySQL solo aplica MYSQL_PASSWORD con el volumen vacío; si ya hay datos, cámbiala con ALTER USER)"

$(APP_ENV): .env
	@umask 077; set -a; . ./.env; set +a; \
		printf 'DB_USER=%s\nDB_PASSWORD=%s\n' "$${DB_USER:-inventory}" "$$DB_PASSWORD" > $@
	@echo "Generado $@"

# imagePullPolicy is Never: the image must already be in the node's image store.
k8s-image:
ifeq ($(CLUSTER),minikube)
	eval $$(minikube docker-env) && \
		docker build --build-context contracts=contracts -t $(IMAGE) inventory-service
else
	docker build --build-context contracts=contracts -t $(IMAGE) inventory-service
endif

# Kept out of base/ and mysql/ so deleting the app never deletes the namespace (and the database with it).
k8s-namespace:
	$(KUBECTL) apply -k k8s/namespace

k8s-validate: k8s-secrets k8s-namespace
	$(KUBECTL) apply -k k8s/mysql --dry-run=server
	$(KUBECTL) apply -k k8s/overlays/minikube --dry-run=server

# minikube (Docker driver on macOS/Windows) opens a tunnel and blocks: keep it open while Postman runs.
k8s-url:
ifeq ($(CLUSTER),minikube)
	minikube service service-inventory -n $(NAMESPACE) --url
else
	@echo "http://localhost:$$($(KUBECTL) get service service-inventory -n $(NAMESPACE) -o jsonpath='{.spec.ports[0].nodePort}')"
endif

# In-cluster load against GET /inventories; watch it with: kubectl get hpa -n geovannycode -w
k8s-load:
	$(KUBECTL) run k6-load -n $(NAMESPACE) --rm -i --restart=Never --image=grafana/k6:2.3.0@sha256:9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34 -- \
		run --vus 50 --duration 3m - < k8s/load/inventories.js

# Removes the app only; MySQL and its data stay.
k8s-down:
	$(KUBECTL) delete -k k8s/overlays/minikube --ignore-not-found

# Destroys everything in the namespace, including the MySQL PVC.
k8s-purge:
	$(KUBECTL) delete namespace $(NAMESPACE) --ignore-not-found

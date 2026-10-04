# Microservicios con Spring Boot y Kubernetes

`docker compose up -d --build` levanta MySQL 8.4 y `service-inventory`, y PostgreSQL 17 para Order (`postgresql`); el perfil `observability` añade Grafana, Loki, Tempo y Prometheus (`grafana/otel-lgtm`). Consulta [Inventory Service](inventory-service/README.md) para configurar `.env`, iniciar la base y ejecutar la aplicación y las verificaciones. Para Order, consulta [Order Service](order-service/README.md).
# microservices-kubernetes

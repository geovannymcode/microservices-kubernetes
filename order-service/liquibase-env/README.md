# Liquibase CLI — Order Service

La CLI usa **el mismo changelog que la aplicación** (`order-service/src/main/resources/db/changelog`), sin copiarlo.

`liquibase.properties` solo define dos cosas:

- `changeLogFile=db/changelog/master.yaml`;
- `searchPath=../src/main/resources`.

Gracias a esto, cada changeSet queda registrado en `DATABASECHANGELOG` con la misma ruta lógica (`db/changelog/...`) que usa la app desde el classpath. Por eso `status` desde la CLI no muestra pendientes después de que la app haya migrado.

La conexión nunca está en el archivo: se pasa con `LIQUIBASE_COMMAND_URL`, `LIQUIBASE_COMMAND_USERNAME` y `LIQUIBASE_COMMAND_PASSWORD`.

## Ejecutarla sin instalarla (Docker Compose)

Desde la raíz del repo, con el `.env` de la raíz completo (`POSTGRES_*`):

```sh
docker compose up -d postgresql
docker compose run --rm liquibase status
```

El servicio `liquibase` (profile `tools`) cumple estas condiciones:

- **Imagen:** se construye desde `liquibase-env/Dockerfile`, que parte de `liquibase/liquibase:5.0.3`, fijada por digest.
- **Driver:** las imágenes de Liquibase 5 no traen drivers JDBC, así que se añade PostgreSQL 42.7.13 desde Maven Central con verificación de checksum.
- **Versiones:** son las mismas que usa la app (Boot 4.1.1 gestiona Liquibase 5.0.3).
- **Contexto por defecto:** `local`, igual que la app (`LIQUIBASE_CONTEXTS`). `--contexts=...` en la línea de comandos lo sobrescribe.
- **Montajes:** monta el changelog y `liquibase.properties` en solo lectura.

Con una instalación local de la CLI:

```sh
cd order-service/liquibase-env
export LIQUIBASE_COMMAND_URL=jdbc:postgresql://localhost:5432/orderdb
export LIQUIBASE_COMMAND_USERNAME=order
export LIQUIBASE_COMMAND_PASSWORD=...
liquibase status --contexts=local
```

## Comandos del curso, en orden seguro

Primero se mira y después se ejecuta: cada cambio se precede del comando que muestra su SQL sin aplicar nada, y se cierra verificando.

| Paso | Comando | Qué hace |
|---|---|---|
| 1. Ver qué hay aplicado | `docker compose run --rm liquibase history` | changeSets aplicados, con fecha y deployment id |
| 2. Ver qué falta | `docker compose run --rm liquibase status --verbose` | changeSets pendientes en el contexto activo (`local` por defecto) |
| 3. Ejecutar | `docker compose run --rm liquibase update` | aplica los pendientes del contexto activo |
| 4. Ver el SQL de otro ambiente | `docker compose run --rm liquibase update-sql --contexts=prod` | imprime el SQL que correría en prod (aquí, el `ALTER TABLE ... ADD reference`), sin ejecutarlo |
| 5. Comparar contextos | `docker compose run --rm liquibase status --contexts=dev --verbose` | en `dev` no aplica ni el 002 (`cert, prod`) ni el seed (`local`): "is up to date" |
| 6. Ver el SQL del rollback | `docker compose run --rm liquibase rollback-count-sql 1` | imprime el rollback del último changeSet aplicado, sin ejecutarlo |
| 7. Deshacer | `docker compose run --rm liquibase rollback-count 1` | ejecuta ese rollback |
| 8. Verificar | `docker compose run --rm liquibase history` y `status --verbose` | confirma el estado final |

Notas:

- **Changesets sin contexto:** se ejecutan en todos los contextos. `contextFilter` solo restringe los que lo declaran (002 → `cert, prod`; 900 → `local`).
- **`--contexts` explícito fuera de Compose:** la CLI *sin* contexto aplica todos los changeSets, incluidos el seed y la columna de cert/prod. Compose fija `local` por defecto; fuera de él, pasa siempre `--contexts`.
- **Rollback del seed:** borra solo las tres filas semilla, que se identifican por sus `created_at` fijos. Las órdenes creadas después por la app no se tocan.
- **Comprobado:** `rollback-count 1` seguido de `update` deja el esquema idéntico (`pg_dump --schema-only` sin diferencias).
- **ChangeSets aplicados:** nunca se editan. Cualquier cambio va en un changeSet nuevo, con su `rollback`, y se incluye en `master.yaml` en su posición.

# Postman — Inventory Service

Importa `services-inventory.postman_collection.json` y configura `baseUrl` (por defecto `http://localhost:8080`). Ejecuta las peticiones de Inventories en orden contra la aplicación de la fase 5 en perfil local. El escenario 409 por stock prepara un producto nuevo con una unidad y solicita dos; no depende del stock de AC-1550.

Esta es una colección nueva: la colección original del curso no estaba en el workspace. Por tanto, quedan pendientes de aplicar sobre el original la corrección de `Notify → readiness` y el traslado de requests `circuitbreakers` de Inventory a Order. La colección nueva ya tiene readiness bajo Inventory y no incluye circuitbreakers de Order.

Los headers `x-api-key` usan `{{apiKey}}` y están deshabilitados por defecto: este servicio aún no exige autenticación. Guarda la clave únicamente en un valor local o secreto de Postman. Si la colección original contiene claves reales, deben revocarse/rotarse en su proveedor; sustituir el texto por una variable no revoca una clave expuesta. No se encontró ninguna clave real en los archivos disponibles.

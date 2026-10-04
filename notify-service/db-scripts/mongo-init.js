// Runs once, on an empty data volume (/docker-entrypoint-initdb.d), as the root user from MONGO_INITDB_ROOT_*.
// Creates the application user in its own database with readWrite only: the service never connects as root.
// Credentials come from the container environment (docker-compose.yaml), never from this file.
const dbName = process.env.MONGO_DB;
const user = process.env.MONGO_USER;
const password = process.env.MONGO_PASSWORD;

if (!dbName || !user || !password) {
  throw new Error('MONGO_DB, MONGO_USER y MONGO_PASSWORD son obligatorias para crear el usuario de la aplicación');
}

const appDb = db.getSiblingDB(dbName);
if (appDb.getUser(user) === null) {
  appDb.createUser({ user: user, pwd: password, roles: [{ role: 'readWrite', db: dbName }] });
  print(`Usuario ${user} creado en ${dbName} con rol readWrite`);
} else {
  print(`El usuario ${user} ya existe en ${dbName}`);
}

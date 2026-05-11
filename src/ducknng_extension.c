#include "duckdb_extension.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_api.h"

DUCKDB_EXTENSION_ENTRYPOINT_CUSTOM(duckdb_extension_info info, struct duckdb_extension_access *access) {
    duckdb_connection connection = NULL;
    ducknng_runtime *rt = NULL;
    duckdb_database *db = NULL;
    int created = 0;
    if (!access || !info) {
        return false;
    }
    db = access->get_database(info);
    if (!db || !*db) {
        access->set_error(info, "ducknng: failed to get database handle");
        return false;
    }
    if (duckdb_connect(*db, &connection) == DuckDBError || !connection) {
        access->set_error(info, "ducknng: failed to open init connection");
        return false;
    }
    if (!ducknng_runtime_init(connection, info, access, &rt, &created)) {
        duckdb_disconnect(&connection);
        return false;
    }
    if (!ducknng_register_sql_api(connection, rt)) {
        access->set_error(info, "ducknng: failed to register sql api");
        if (created) {
            ducknng_runtime_destroy(rt);
        } else {
            duckdb_disconnect(&connection);
        }
        return false;
    }
    /* Log capture is enabled at runtime via SELECT ducknng_enable_log_capture()
     * rather than here. Calling duckdb_register_log_storage during extension
     * load triggers a DuckDB v1.5.2 internal assertion failure. */
    if (!created) {
        duckdb_disconnect(&connection);
    }
    return true;
}

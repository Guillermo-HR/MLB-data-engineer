# Seleccionar entorno de ejecución
dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Selecciona el Entorno")
env = dbutils.widgets.get("entorno")
print(f"Ejecutando pipeline en el entorno: {env}")

# Crear esquemas
print("Crear esquemas para MLB")
if env == "prod":
    print("Esquemas producción")
    spark.sql("CREATE DATABASE IF NOT EXISTS mlb_prod_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS mlb_prod_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS mlb_prod_gold")
elif env == "dev": 
    print("Esquemas develop")
    spark.sql("CREATE DATABASE IF NOT EXISTS mlb_dev_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS mlb_dev_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS mlb_dev_gold")

print("Creacion de esquemas finalizada")

# Crear tablas
print("Crear tablas para MLB")
if env == "prod":
    print("Tablas producción")
elif env == "dev":
    print("Tablas develop")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_{env}_bronze.game_schedule (
            game_pk STRING,
            home_team STRING,
            away_team STRING,
            game_scheduled_time TIMESTAMP,
            status STRING,
            ingestion_timestamp TIMESTAMP
            ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_{env}_bronze.game_data (
            game_pk STRING,
            metadata STRING,
            live_data STRING,
            ingestion_timestamp TIMESTAMP
            ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_{env}_bronze.failed_game_schedule (
              response STRING,
              ingestion_timestamp TIMESTAMP
              ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_{env}_bronze.failed_game_data (
            game_pk STRING,
            response STRING,
            ingestion_timestamp TIMESTAMP
            ) USING DELTA""")

print("Creacion de tablas finalizada")
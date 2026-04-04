# Crear esquemas
print("Create schemas")
print("Schemas prod")
spark.sql("CREATE DATABASE IF NOT EXISTS mlb_prod_bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS mlb_prod_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS mlb_prod_gold")

print("Schemas dev")
spark.sql("CREATE DATABASE IF NOT EXISTS mlb_dev_bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS mlb_dev_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS mlb_dev_gold")

print("Create schemas completed")

# Crear tablas
print("Create tables")
print("Tables prod")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_prod_bronze.game_schedule (
        game_pk STRING,
        home_team STRING,
        away_team STRING,
        game_scheduled_time TIMESTAMP,
        status STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_prod_bronze.game_data (
        game_pk STRING,
        response STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_prod_bronze.failed_game_schedule (
        response STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_prod_bronze.failed_game_data (
        game_pk STRING,
        response STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
        
print("Tables dev")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_dev_bronze.game_schedule (
        game_pk STRING,
        home_team STRING,
        away_team STRING,
        game_scheduled_time TIMESTAMP,
        status STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_dev_bronze.game_data (
        game_pk STRING,
        response STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_dev_bronze.failed_game_schedule (
        response STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS mlb_dev_bronze.failed_game_data (
        game_pk STRING,
        response STRING,
        ingestion_timestamp TIMESTAMP
        ) USING DELTA""")

print("Create tables completed")
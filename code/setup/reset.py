print("Eliminar esquemas para MLB")

print("Eliminando esquemas producción")
spark.sql("DROP DATABASE IF EXISTS mlb_prod_bronze CASCADE")
spark.sql("DROP DATABASE IF EXISTS mlb_prod_silver CASCADE")
spark.sql("DROP DATABASE IF EXISTS mlb_prod_gold CASCADE")

print("Eliminando esquemas develop")
spark.sql("DROP DATABASE IF EXISTS mlb_dev_bronze CASCADE")
spark.sql("DROP DATABASE IF EXISTS mlb_dev_silver CASCADE")
spark.sql("DROP DATABASE IF EXISTS mlb_dev_gold CASCADE")

print("Eliminación de esquemas finalizada")
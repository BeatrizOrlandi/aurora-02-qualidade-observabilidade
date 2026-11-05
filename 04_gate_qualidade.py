# Databricks notebook source
# MAGIC %md
# MAGIC # Bloquear a publicação se a qualidade crítica falhar
# MAGIC Dados próprios; execução nativa; sem pip ou downloads. Use o Job `Aurora — pipeline de dados completo` para executar o fluxo.

# COMMAND ----------
import json, sys
sys.path.insert(0, "/Workspace/Users/<SEU_USUARIO>/portfolio-estudos/01_lakehouse_aurora/runtime")
from aurora_runtime import *
spark.conf.set("spark.sql.session.timeZone", "UTC")
current_user = spark.sql("SELECT current_user() AS user").first().user
assert current_user, "Usuário autenticado necessário"
config = from_widgets(dbutils)

# COMMAND ----------
result = quality_gate(spark, config)
print(json.dumps(result, ensure_ascii=False, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))

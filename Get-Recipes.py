# Databricks notebook source
# MAGIC %pip install azure-search-documents --pre
# MAGIC %pip show azure-search-documents

# COMMAND ----------

API_KEY = dbutils.widgets.get("APIKey")
CognitiveServiceName = dbutils.widgets.get("CognitiveServiceName")
CognitiveIndexName = dbutils.widgets.get("CognitiveIndexName")
ElastaSQLUsername = dbutils.widgets.get("ElastaSQLUsername")
ElastaSQLPassword = dbutils.widgets.get("ElastaSQLPassword")
ElastaSQLServerURL = dbutils.widgets.get("ElastaSQLServerURL")
ElastaSQLDBName = dbutils.widgets.get("ElastaSQLDBName")
ADBScope = dbutils.widgets.get("ADBScope")
Num_Days = int(dbutils.widgets.get("Num_Days"))

# COMMAND ----------

# MAGIC %run ./Cognitive-Search

# COMMAND ----------

# dbutils.widgets.text("ElastaSQLUsername","")
# dbutils.widgets.text("ElastaSQLServerURL","")
# dbutils.widgets.text("ElastaSQLDBName","")
# dbutils.widgets.text("ElastaSQLPassword","")
# dbutils.widgets.text("ADBScope","")

# COMMAND ----------

configdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
configjdbcurl = "jdbc:sqlserver://{}:1433;databaseName={};useNTLMV2=True".format(ElastaSQLServerURL,ElastaSQLDBName)
configusername = dbutils.secrets.get(ADBScope,ElastaSQLUsername)
configpassword = dbutils.secrets.get(ADBScope,ElastaSQLPassword)

# COMMAND ----------

configconnection_properties = {
    "driver" : configdriver,
    "user" : configusername,
    "password" : configpassword
}

# COMMAND ----------

def getData_sql(sql_query):
    df = spark.read.format("jdbc").option("url",configjdbcurl).option("user",configusername).option("password",configpassword).option("query",sql_query).load()
    return df

# COMMAND ----------

get_foods = "select * from Foods"
foods_df = getData_sql(get_foods)

# COMMAND ----------

display(foods_df)

# COMMAND ----------

from datetime import date
from datetime import timedelta

# COMMAND ----------

search_date = date.today() + timedelta(days = Num_Days)
print(search_date)

# COMMAND ----------

refined_foods_df = foods_df.where(foods_df.UseByDate == search_date)
food_name_list = (refined_foods_df.select(refined_foods_df.Name).distinct().rdd.flatMap(lambda x:x).collect())

# COMMAND ----------

print(food_name_list)

# COMMAND ----------

final = {}

# COMMAND ----------

for items in food_name_list:
    item_list = []
    item = items[:-1] + "*"
    value = search(item)
    print(value)
    id = value['RecipeID']
    Ingredients = value['Ingredients']
    item_list.append(id)
    item_list.append(Ingredients)
    final[items] = item_list

# COMMAND ----------

dbutils.notebook.exit(str(final))

# Databricks notebook source
import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient 
from azure.search.documents import SearchClient
from azure.search.documents.indexes.models import (
    ComplexField,
    CorsOptions,
    SearchIndex,
    ScoringProfile,
    SearchFieldDataType,
    SimpleField,
    SearchableField
)

# COMMAND ----------

admin_key = dbutils.secrets.get(ADBScope,API_KEY)

endpoint = "https://{}.search.windows.net/".format(CognitiveServiceName)
search_client = SearchClient(endpoint=endpoint,
                      index_name=CognitiveIndexName,
                      credential=AzureKeyCredential(admin_key))

# COMMAND ----------

def search(item):
    output = search_client.search(search_text = item, select = 'RecipeID,Ingredients')
    for col in output:
        return col

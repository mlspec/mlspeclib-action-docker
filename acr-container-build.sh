#!/bin/sh

ACR_NAME='mlspeclibdocker'
RES_GROUP='gha_and_aml_rg'
# az group create --resource-group $RES_GROUP --location eastus
# az acr create --resource-group $RES_GROUP --name $ACR_NAME --sku Standard --location eastus

# az acr build --registry $ACR_NAME --image mlspecdocker:v1 .
gcloud builds submit --tag gcr.io/scorpio-216915/mlspeclibdocker
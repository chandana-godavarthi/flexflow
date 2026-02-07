#!/bin/bash
pipx install poetry
pipx ensurepath
poetry config virtualenvs.in-project true
poetry config certificates.pg_jfrog.cert /etc/ssl/certs/ca-certificates.crt
echo 'Authenticate to Poetry with:'
echo '    poetry config http-basic.pg_jfrog <username> <pat>'
echo 'Install dependencies with:'
echo '    poetry install'
echo 'Authenticate to Databricks with:'
echo '    az login'
echo '	or'
echo '    databricks configure'
echo 'Set Databricks Cluster with:'
echo '    export DATABRICKS_CLUSTER_ID=xxxxxxxxxx'
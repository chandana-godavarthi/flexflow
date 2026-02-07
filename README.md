# tp_dp_ff_pipelines

[![DAB Template](https://img.shields.io/badge/Databricks_Asset_Bundle_Template-Pipeline-blue)](https://github.com/procter-gamble/de-cf-databricks-pipeline-template)
<!--find/replace <REPO_NAME> with your repository name and uncomment to fancy gh actions badge-->
<!--
[![Build, Analyze, Test, Publish](https://github.com/procter-gamble/<REPO_NAME>/actions/workflows/ci.yml/badge.svg)](https://github.com/procter-gamble/<REPO_NAME>/actions/workflows/ci.yml)
-->
<!--find/replace <REPO_NAME> with your repository name, <SONAR_TOKEN> with sonar token, and uncomment to fancy sonarqube badges-->
<!--
[![Quality Gate Status](https://sonarqubeenterprise.pgcloud.com/sonarqube/api/project_badges/measure?project=<REPO_NAME>&metric=alert_status&token=<SONAR_TOKEN>)](https://sonarqubeenterprise.pgcloud.com/sonarqube/dashboard?id=<REPO_NAME>)
[![Maintainability Rating](https://sonarqubeenterprise.pgcloud.com/sonarqube/api/project_badges/measure?project=<REPO_NAME>&metric=sqale_rating&token=<SONAR_TOKEN>)](https://sonarqubeenterprise.pgcloud.com/sonarqube/dashboard?id=<REPO_NAME>)
[![Coverage](https://sonarqubeenterprise.pgcloud.com/sonarqube/api/project_badges/measure?project=<REPO_NAME>&metric=coverage&token=<SONAR_TOKEN>)](https://sonarqubeenterprise.pgcloud.com/sonarqube/dashboard?id=<REPO_NAME>)
-->
<!--find/replace <REPO_NAME> with your repository name and uncomment to fancy snyk badge-->
<!--
[![Known Vulnerabilities](https://snyk.io/test/github/procter-gamble/<REPO_NAME>/badge.svg)](https://snyk.io/test/github/procter-gamble/<REPO_NAME>)
-->


## Pipeline Documentation
   - [Technical](docs/technical.md)
   - [Operations](docs/operations.md)
   - [Governance](docs/governance.md)
   - [Release Notes](docs/release-notes.md)

## Getting started


1. Clone this repository to a git folder in your workspace Create=>Git Folder, enter the https path to the repo

2. Configure your all purpose compute cluster to dbr >= 15.4lts

3. Install & configure Dependencies
   Ensure your development all purpose cluster is configured with `PIP_EXTRA_INDEX_URL` Environment variable in the format: `https://<user>:<token>@png.jfrog.io/artifactory/api/pypi/pgg-python/simple/`
   
4. Develop your etl & tests (.py) and ddl (.sql)
   - Add configuration to `tp_dp_ff_pipelines/tp_dp_ff_pipelines/config/*.yaml` and tests/config/test.yaml for any values which change per environment or infrastructure.
   - Add any secrets you need to access to your yaml config using the form {{secrets/scope-name/key-name}} for your values and they will be resolved using dbutils
   - Add any package dependencies to your notebooks using `pip install` or us `libraries` in the task properties of your databricks yaml.
   - Develop your notebooks in `tp_dp_ff_pipelines/notebooks`
   - Define your workflow in `resources/*.yml`, be sure to set your `run_as` identity or bundle deployment will fail.  [databricks yaml reference](https://docs.databricks.com/en/dev-tools/bundles/settings.html).  You may also use the web terminal and invoke `databricks bundle job generate --existing-job-id <your job id>`
   - Define your schema migrations in `tp_dp_ff_pipelines/tp_dp_ff_pipelines/ddl/####-*.sql`. We recommend numbering them with left padded zeroes to define order.  Run your migrations `poetry run migrate`
   - Run your workflow in databricks using 
      ```bash
      $ databricks bundle deploy
      $ databricks bundle run`
      ```


5. Deploy
   - To deploy a non-production copy, type:
      ```bash
      $ databricks bundle deploy --target nonprod
      ```

   - To Deploy a production copy of this project, type:
      ```
      $ databricks bundle deploy --target prod
      ```

6. Configure CI/CD
   - Add the following secrets to your github repo
      - `JFROG_USER` P&G JFrog Username for downloading python libraries [Get JFrog Access](https://pgone.sharepoint.com/sites/TechnicalITDocumentation/Shared%20Documents/devdocs/cicd/jfrog/how-to/get_jfrog_access/)
      - `JFROG_TOKEN` P&G JFrog Token for downloading python libraries
      - `SNYK_TOKEN` token for snyk
      - `SONAR_TOKEN` token to access P&G SonarQube
      - `AZURE_TENANT_ID` the P&G Azure Tenant ID
   - Add the following Variables to your github repo
      - `SONAR_PROJECT_KEY` Project Key for SonarQube (You will need to create your project in SonarQube UI)
      - `SONAR_HOST_URL` P&G SonarQube URL: https://sonarqubeenterprise.pgcloud.com/sonarqube
   - For each of your deployment environments
      - Secrets
         - `AZURE_CLIENT_ID` - Client ID to your federated Azure SP
      - Variables
         - `WORKSPACE_CLUSTER_ID` - ID of cluster to use to execute schema migrations
         - `WORKSPACE_URL` - URL of workspace to use for deployment

## Documentation & References
   - Databricks Asset Bundles (databricks.yaml, resources/*.yml) - https://docs.databricks.com/dev-tools/bundles/index.html
   
   - Configuration (config/*) - https://github.com/procter-gamble/de-cf-ps-configuration
   - Migration (ddl/*.sql) - https://github.com/procter-gamble/de-cf-ps-migration
   - CDL Publication - https://github.com/procter-gamble/de-cf-ps-cdl
   - PySpark - https://spark.apache.org/docs/latest/api/python/index.html
   - Chispa - https://github.com/MrPowers/chispa

<!--
This repo was initialized from a dab template located at https://github.com/procter-gamble/de-cf-databricks-template
You can recreate it's initial state by creating a json file dab.json as
{
    "project_name": "tp_dp_ff_pipelines",
    "schema_name": "tp_dp_ff_pipelines",
    "migrate": "no",
    "publish_type": "unity-catalog+ddapi",
    "das_rls": "yes",
    "developer_workflow": "ui-notebook",
    "cicd_workflow": "github",
    "environments":"dev,prod",
    "ado_variable_group_name":"",
    "ado_pipeline_name":"databricks-ci-cd-pipeline",
    "ado_environment_staging":"",
    "ado_environment_production":"",
    "dev_workspace":"https://adb-3597259230236866.6.azuredatabricks.net/?o=3597259230236866",
    "dev_catalog":"cdl_tp_dev",
    "dev_service_principal_name":"az-sp-cdl-flexflow-tp-01-dev",
    "dev_secret_scope":"KeyVault_Scope",
    "dev_cluster_policy_name": "default-flexflow-cluster-policy",
    "dev_instance_pool_name": "",
    "uat_workspace":"",
    "uat_catalog":"hive_metastore",
    "uat_service_principal_name":"",
    "uat_secret_scope":"",
    "uat_cluster_policy_name": "",
    "uat_instance_pool_name": "",
    "prod_workspace":"https://adb-2472717941295173.13.azuredatabricks.net/?o=2472717941295173",
    "prod_catalog":"cdl_prod_dev",
    "prod_service_principal_name":"az-sp-cdl-flexflow-tp-01-prod",
    "prod_secret_scope":"KeyVault_Scope",
    "prod_cluster_policy_name": "default-flexflow-cluster-policy",
    "prod_instance_pool_name": ""
}
and execute databricks bundle init https://github.com/procter-gamble/de-cf-databricks-template --template-dir dab --config-file=dab.json
-->
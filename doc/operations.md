# Operations

## Key Contacts
* Product Owner:
* Dev team(L3 support):

## Azure Resources Used
### Dev
* Databricks workspace: `https://adb-3597259230236866.6.azuredatabricks.net/?o=3597259230236866`
* Azure Subscription: 
* Azure Resource group: 
* Azure storage account: 
* Azure storage container:
* service principal: az-sp-cdl-flexflow-tp-01-dev
* Azure Key Vault: KeyVault_Scope
### Prod
* Databricks workspace: `https://adb-2472717941295173.13.azuredatabricks.net/?o=2472717941295173`
* Azure Subscription: 
* Azure Resource group: 
* Azure storage account: 
* Azure storage container:
* service principal: az-sp-cdl-flexflow-tp-01-prod
* Azure Key Vault: KeyVault_Scope

## Regularly Scheduled Maintenance
### Changing passwords/tokens
If the password/token expires, please create a new one and upload it in Azure KeyVault. 
> Note that there are no pipeline-speciffic secrets for this application.

## Common Problems & Solutions
### Fail scenario
After workflow failure an email is sent to the defined group(s). The list of emails addresses is set up in the databricks asset bundle [job definition](/resources/tp_dp_ff_pipelines_job.yml) There is no need to remove any data before the restart. You can simply rerun the process from where it failed.

For CDL publication issues contact DDAPI support (applies only to `publish_task` of the pipeline).

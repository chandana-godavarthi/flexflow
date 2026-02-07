<#
 .Synopsis
  Sets Azure DevOps Pipeline tags based on the source branch trigger

 .Description
  Sets Azure DevOps Pipeline tags based on the source branch trigger

 .Parameter SourceBranch
  A parameter type of string with name of the source branch

 .Example
  Set-BuildPipelineTags.ps1 -SourceBranch "develop"
#>

param (
    [Parameter(Mandatory = $true)]
    [string]$SourceBranch
)

Write-Host "`n -------------------------------------------------------------------"
Write-Host " [INFO] Source branch: $($SourceBranch)"
Write-Host " [INFO] In progress, please wait..."
Write-Host " -------------------------------------------------------------------`n"

$branch = $($SourceBranch).ToLower()
Write-Host "##vso[build.addbuildtag]app_branch=$($branch)"

if($branch -like "*/develop"){
  Write-Host "##vso[build.addbuildtag]target_environment=development"
  Write-Host "##vso[build.addbuildtag]stage=development"
  Write-Host "##vso[build.addbuildtag]development"
}
elseif ($branch -like "*/main" -or $branch -like "*/master") {
  Write-Host "##vso[build.addbuildtag]target_environment=production"
  Write-Host "##vso[build.addbuildtag]stage=production"
  Write-Host "##vso[build.addbuildtag]production"
}
elseif ($branch -like "*release/*" -or $branch -like "*hotfix/*") {
  Write-Host "##vso[build.addbuildtag]target_environment=uat"
  Write-Host "##vso[build.addbuildtag]stage=uat"
  Write-Host "##vso[build.addbuildtag]uat"
}
else {
  Write-Host "##vso[build.addbuildtag]target_environment=custom"
  Write-Host "##vso[build.addbuildtag]stage=custom"
  Write-Host "##vso[build.addbuildtag]custom"
}

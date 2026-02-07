<#
.SYNOPSIS
    This script sets the build version based on the branch name and other parameters.

.PARAMETER BranchName
    The BranchName parameter specifies the name of the branch.

.PARAMETER fullSemVer
    The fullSemVer parameter specifies the full semantic version.

.PARAMETER MajorMinorPatch
    The MajorMinorPatch parameter specifies the major, minor, and patch version.

.PARAMETER PreReleaseNumber
    The PreReleaseNumber parameter specifies the pre-release number.

.PARAMETER initFilePath
    The initFilePath parameter specifies the path to the __init__.py file.

.PARAMETER tomlFilePath
    The tomlFilePath parameter specifies the path to the .toml project file.

.EXAMPLE
    .\Set-BuildVersion.ps1 -BranchName "feature/new-feature" -fullSemVer "1.0.0" -MajorMinorPatch "1.0.0" -PreReleaseNumber "1" -initFilePath "path\to\__init__.py" -tomlFilePath "path\to\project.toml"
    
    This command sets the build version based on the specified parameters.
#>
param (
    [Parameter(Mandatory = $true)]
    [string]$BranchName,
    [Parameter(Mandatory = $true)]
    [string]$fullSemVer,
    [Parameter(Mandatory = $true)]
    [string]$MajorMinorPatch,
    [Parameter(Mandatory = $false)]
    [string]$PreReleaseNumber,
    [Parameter(Mandatory = $true)]
    [string]$initFilePath,
    [Parameter(Mandatory = $true)]
    [string]$tomlFilePath
)

Write-Host "[INFO] Branch name: $BranchName"

if ($BranchName -notmatch "^(main|master|develop|hotfix|release/.*)$") {
    $version = "__version__ = `"$MajorMinorPatch-dev.$PreReleaseNumber`""
    $version_toml = "$MajorMinorPatch-dev.$PreReleaseNumber"
}
else {
    if ($fullSemVer -match "PullRequest") {
        $version = "__version__ = `"$MajorMinorPatch-dev.$PreReleaseNumber`""
        $version_toml = "$MajorMinorPatch-dev.$PreReleaseNumber"
    }
    else {
        $version = "__version__ = `"$fullSemVer`""
        $version_toml = "$fullSemVer"
    }
}

Write-Host "[INFO] Update version to: $version"
Write-Host "[INFO] Update toml version to: $version_toml"

# Update version in __init__.py file
try {
    Set-Content -Path $initFilePath -Value $version -Force
    Write-Host "[INFO] Successfully updated version in __init__.py file"
}
catch {
    Write-Host "[WARN] Failed to update version in __init__.py file: $_"
}

# Update version in .toml project file
try {
    if (-not (Get-Module -ListAvailable -Name PSToml)) {
        Install-Module -Name PSToml -Scope CurrentUser -Force
    }
}
catch {
    Write-Error "[ERROR] Failed to install PSToml module: $_"
    exit 1
}

try {
    $toml = ConvertFrom-Toml -InputObject (Get-Content -Path $tomlFilePath)
    $toml.project.version = $version_toml
    $toml | ConvertTo-Toml -Depth 10 | Set-Content -Path $tomlFilePath
    Write-Host "[INFO] Successfully updated version in .toml project file"
}
catch {
    Write-Error "[ERROR] Failed to update version in .toml project file: $_"
    exit 1
}
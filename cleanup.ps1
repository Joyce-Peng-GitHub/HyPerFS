Set-Location -Path $PSScriptRoot

$targets = @("tmp", "data", "db")

foreach ($name in $targets) {
	if (Test-Path $name) {
		Write-Host "Cleaning: $name ..." -ForegroundColor Cyan
		try {
			Remove-Item -Path $name -Recurse -Force -ErrorAction Stop
			Write-Host "Successfully deleted: $name" -ForegroundColor Green
		}
		catch {
			Write-Warning "Failed to delete $name : $($_.Exception.Message)"
		}
	}
 else {
		Write-Host "Skip: $name (Not found)" -ForegroundColor Gray
	}
}

Write-Host "`nAll operations completed!" -ForegroundColor Magenta
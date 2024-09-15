param (
    [string]$Path,  # Default file path
    [int]$Size,  # Default size
    [ValidateSet("B", "KB", "MB", "GB")]
    [string]$Unit = "B"  # Default unit
)

# Convert size to bytes based on the specified unit
switch ($Unit) {
    "B"  { $sizeInBytes = $Size }
    "KB" { $sizeInBytes = $Size * 1KB }
    "MB" { $sizeInBytes = $Size * 1MB }
    "GB" { $sizeInBytes = $Size * 1GB }
    default { throw "Invalid unit specified." }
}

# Create a byte array of the specified size
$content = New-Object byte[] $sizeInBytes

# Fill the array with random bytes
(New-Object System.Random).NextBytes($content)

# Write the byte array to a file
[System.IO.File]::WriteAllBytes($Path, $content)

Write-Host "File created at $Path with size $Size $Unit"
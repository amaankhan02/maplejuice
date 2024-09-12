# Arguments
param (
    [int]$NUM_CONTAINERS = 2  # Default if no argument is provided
)

Write-Host "Gathering output files from Docker containers...`n"

# Create a directory to store the output files
$outputDir = "container_outputs"
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

for ($i = 1; $i -le $NUM_CONTAINERS; $i++) {
    # Format the container number with leading zero
    $CONTAINER_NUM = "{0:D2}" -f $i

    # Construct the source and destination paths
    $sourceFile = "/src/output/output.log"
    $destFile = "$outputDir/mj-vm-$CONTAINER_NUM-output.log"

    # Copy the output file from the container to the host
    Write-Host "Copying output file from container mj-vm-$CONTAINER_NUM..."
    docker cp "mj-vm-$CONTAINER_NUM`:$sourceFile" $destFile
}

Write-Host "`nOutput files gathered successfully in the '$outputDir' directory."
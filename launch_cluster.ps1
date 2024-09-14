# Arguments
param (
    [int]$NUM_CONTAINERS = 4  # Default if no argument is provided
)

Write-Host "Building docker image..."
$buildResult = docker build -t maplejuice-image .
$buildResultOutput = $buildResult | Format-List | Out-String
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to build docker image. Exiting script. Here is the output from 'docker build' command"
    Write-Error $buildResultOutput
    exit 1
}
# if ($LASTEXITCODE -ne 0) {
#     Write-Error "Failed to build docker image. Exiting script."
#     exit 1
# }
# try {
#     docker build -t maplejuice-image .
# } catch {
#     Write-Error "Failed to build docker image. Exiting script."
#     exit 1
# }


# Check if the network exists
$networkExists = docker network ls --format '{{.Name}}' | Where-Object { $_ -eq 'maplejuice-net' }

# Create the network if it doesn't exist
if (-not $networkExists) {
    Write-Host "`nCreating maplejuice-net network..."
    docker network create maplejuice-net
} else {
    Write-Host "`nmaplejuice-net network already exists."
}

Write-Host "`nCreating Docker volumes...`n"
for ($i = 1; $i -le $NUM_CONTAINERS; $i++) {
    # Format the container number with leading zero
    $CONTAINER_NUM = "{0:D2}" -f $i

    # Create the Docker volume
    $VOLUME_NAME = "mj-vm-$CONTAINER_NUM-output"
    Write-Host "Creating volume $VOLUME_NAME..."
    docker volume create $VOLUME_NAME
}

Write-Host "`nLaunching $NUM_CONTAINERS Docker containers...`n"
for ($i = 1; $i -le $NUM_CONTAINERS; $i++) {
    # Format the container number with leading zero
    $CONTAINER_NUM = "{0:D2}" -f $i

    # Construct the command to run
    $VOLUME_NAME = "mj-vm-$CONTAINER_NUM-output"
    $CMD = "docker run -it --rm --name mj-vm-$CONTAINER_NUM --hostname VM$CONTAINER_NUM --network maplejuice-net -v ${VOLUME_NAME}:/src/app_data maplejuice-image"

    # Start a new PowerShell window and run the command
    Write-Host "Launching container mj-vm-$CONTAINER_NUM..."
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $CMD
    Start-Sleep -Milliseconds 250
}
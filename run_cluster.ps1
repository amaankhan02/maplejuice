
# Arguments
param (
    [int]$NUM_CONTAINERS = 2  # Default if no argument is provided
)

Write-Host "Building docker image..."
docker build -t maplejuice-image .


# Check if the network exists
$networkExists = docker network ls --format '{{.Name}}' | Where-Object { $_ -eq 'maplejuice-net' }

# Create the network if it doesn't exist
if (-not $networkExists) {
    Write-Host "`nCreating maplejuice-net network..."
    docker network create maplejuice-net
} else {
    Write-Host "`nmaplejuice-net network already exists."
}

Write-Host "`nLaunching $NUM_CONTAINERS Docker containers...`n"
for ($i = 1; $i -le $NUM_CONTAINERS; $i++) {
    # Format the container number with leading zero
    $CONTAINER_NUM = "{0:D2}" -f $i

    # Construct the command to run
    $CMD = "docker run -it --rm --name mj-vm-$CONTAINER_NUM --hostname vm$CONTAINER_NUM --network maplejuice-net maplejuice-image"

    # Start a new PowerShell window and run the command
    Write-Host "Launching container mj-vm-$CONTAINER_NUM..."
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $CMD
    Start-Sleep -Milliseconds 250
}
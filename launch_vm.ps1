# Arguments
param (
    [int]$CONTAINER_NUM,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$OptionalArgs
)

# Validate the container number is provided
if (-not $CONTAINER_NUM) {
    Write-Host "Error: You must provide a container number as an argument."
    exit 1
}

# Format the container number with leading zero
$CONTAINER_NUM_FORMATTED = "{0:D2}" -f $CONTAINER_NUM

# Check if the Docker volume exists
$VOLUME_NAME = "mj-vm-$CONTAINER_NUM_FORMATTED-output"
# $volumeExists = docker volume ls --format '{{.Name}}' | Where-Object { $_ -eq $VOLUME_NAME }

# Create the Docker volume (if it already exists, this will do nothing)
Write-Host "Creating volume $VOLUME_NAME if it doesn't already exist..."
docker volume create $VOLUME_NAME

# Join the optional arguments into a single string
$OptionalArgsString = $OptionalArgs -join " "


# Construct the command to run
$CMD = "docker run -it --rm --name mj-vm-$CONTAINER_NUM_FORMATTED --hostname VM$CONTAINER_NUM_FORMATTED --network maplejuice-net -v ${VOLUME_NAME}:/src/app_data maplejuice-image $OptionalArgsString"

# Launch the container
Write-Host "Launching container mj-vm-$CONTAINER_NUM_FORMATTED..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", $CMD
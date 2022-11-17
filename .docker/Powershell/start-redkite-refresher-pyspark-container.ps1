param (
    [string]$m="4g",
    [System.Boolean]$h=$False,
    [System.Boolean]$d=$False
 )

function Help {
   write-host  "Start the docker environment."
   write-host 
   write-host  "Syntax: scriptName -d $True | -h $True | -m 4g"
   write-host
   write-host  "options:"
   write-host  "d     Start the container is disconnected mode. You can connect to the container using 'docker exec -it pyspark-pipeline-container bash'."
   write-host  "m     Max memory the container can use. Should be a number followed by b, k, m, g, to indicate bytes, kilobytes, megabytes, or gigabytes. Default is 4g."
   write-host  "h     Print this Help."
   write-host
}

$DOCKER_DIR=$PSScriptRoot + "/../Dockerfile"
$APP_ROOT_DIR= $PSScriptRoot + "../../../"
$MAX_MEMORY = $m
$DOCKER_RUN_DISCONNECTED=$d

if($h){
	Help
	exit
}

docker rm redkite-refresher-pyspark 2>$null

# Sometimes the container takes a few seconds to come down properly when running VSCode connected
# to the container, so wait a few seconds...
Start-Sleep 3

if (-Not $DOCKER_RUN_DISCONNECTED) {
    write-host "Entering container shell (Type 'exit' to stop the container)..."
	docker run -it `
    --name redkite-refresher-pyspark `
    -m $MAX_MEMORY `
    -v ${APP_ROOT_DIR}/:/workspace/app  `
	-p 4040:4040 `
    --init `
    redkite-refresher-pyspark
	
}else {
    docker run -dit `
    --name redkite-refresher-pyspark `
    -m $MAX_MEMORY `
    -v ${APP_ROOT_DIR}/:/workspace/app  `
	-p 4040:4040 `
    --init `
    redkite-refresher-pyspark
	
	docker ps
}

write-host "Setting GIT config for remote container..."
git config --global credential.https://dev.azure.com.useHttpPath true
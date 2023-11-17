#!/bin/bash
SOURCE_CONTAINER=neo4j-source
# Run the main container command
docker-compose up -d
# Function to check Neo4j server status
check_neo4j_status() {
    docker-compose exec $SOURCE_CONTAINER neo4j-admin server status > /dev/null 2>&1
    return $?
}

enable_cdc() {
    docker-compose exec $SOURCE_CONTAINER cypher-shell -p password -u neo4j "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL'"
}

# Maximum number of attempts
max_attempts=10

# Sleep interval between attempts (in seconds)
sleep_interval=5

attempt=1

# Loop until successful status or maximum attempts reached
while [ $attempt -le $max_attempts ]; do
    echo "Checking Neo4j status (Attempt $attempt)..."
    
    check_neo4j_status
    STATUS=$?
    
    # Check the exit status of the function
    if [ $STATUS -eq 0 ]; then
        echo "Neo4j is running successfully."
        enable_cdc
        break
    else
        echo "Neo4j returned status $STATUS. Retrying in $sleep_interval seconds..."
        sleep $sleep_interval
        ((attempt++))
    fi
done

# Check if maximum attempts reached
if [ $attempt -gt $max_attempts ]; then
    echo "Maximum attempts reached. Neo4j may not be running successfully."
fi

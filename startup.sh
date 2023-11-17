#!/bin/bash
SOURCE_CONTAINER=neo4j-source
SINK_CONTAINER=neo4j-sink
# Run the main container command
docker-compose up -d
# Function to check Neo4j server status
function check_neo4j_status {
    local container=$1 
    docker-compose exec $container cypher-shell -p password -u neo4j "MATCH (n) RETURN n" > /dev/null 2>&1
    return $?
}

# Tells the source database to track changes in its transaction log
function enable_cdc {
    docker-compose exec $SOURCE_CONTAINER cypher-shell -p password -u neo4j "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL'"
}

# Creates the database schema based on commands in migrations.cypher
function setup_schema {
    local container=$1
    docker-compose exec $container cypher-shell -p password -u neo4j -f /migrations/migrations.cypher
}

# Maximum number of attempts
max_attempts=10

# Sleep interval between attempts (in seconds)
sleep_interval=5

attempt=1

# Setup the source instance
while [ $attempt -le $max_attempts ]; do
    echo "Checking Neo4j status (Attempt $attempt)..."
    
    check_neo4j_status $SOURCE_CONTAINER
    STATUS=$?
    
    # Check the exit status of the function
    if [ $STATUS -eq 0 ]; then
        echo "Neo4j source is running successfully."
        enable_cdc
        setup_schema $SOURCE_CONTAINER
        break
    else
        echo "Neo4j source returned status $STATUS. Retrying in $sleep_interval seconds..."
        sleep $sleep_interval
        ((attempt++))
    fi
done

# Check if maximum attempts reached
if [ $attempt -gt $max_attempts ]; then
    echo "Maximum attempts reached. Neo4j source may not be running successfully."
fi

# Setup the sink instance
while [ $attempt -le $max_attempts ]; do
    echo "Checking Neo4j status (Attempt $attempt)..."
    
    check_neo4j_status $SINK_CONTAINER
    STATUS=$?
    
    # Check the exit status of the function
    if [ $STATUS -eq 0 ]; then
        echo "Neo4j sink is running successfully."
        setup_schema $SINK_CONTAINER
        break
    else
        echo "Neo4j sink returned status $STATUS. Retrying in $sleep_interval seconds..."
        sleep $sleep_interval
        ((attempt++))
    fi
done

# Check if maximum attempts reached
if [ $attempt -gt $max_attempts ]; then
    echo "Maximum attempts reached. Neo4j source may not be running successfully."
fi
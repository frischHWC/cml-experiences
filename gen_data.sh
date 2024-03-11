#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#!/usr/bin/env bash

# DATAGEN_URL should be in this form: https://bootcamp-1.vpc.cloudera.com:4242
export DATAGEN_URL="https://localhost:4242"
export DATAGEN_USER="admin"
export DATAGEN_PASSWORD="admin"

generate_data() {
SINK=$1
MODEL_FILE=$2
BATCHES=$3
ROWS=$4
THREADS=$5

echo "Launching command for model: ${MODEL_FILE} to server ${DATAGEN_URL}"

COMMAND_ID=$(curl -s -k -X POST -H "Accept: */*" -H "Content-Type: multipart/form-data ; boundary=toto" \
    -F "model_file=@${MODEL_FILE}" -u ${DATAGEN_USER}:${DATAGEN_PASSWORD} \
    "${DATAGEN_URL}/datagen/${SINK}/?batches=${BATCHES}&rows=${ROWS}&threads=${THREADS}" | jq -r '.commandUuid' )

echo "Checking status of the command"
while true
do
    STATUS=$(curl -s -k -X POST -H "Accept: application/json" -u ${DATAGEN_USER}:${DATAGEN_PASSWORD} \
        "${DATAGEN_URL}/command/getCommandStatus?commandUuid=${COMMAND_ID}" | jq -r ".status")
    printf '.'
    if [ "${STATUS}" == "FINISHED" ]
    then
        echo ""
        echo "SUCCESS: Command for model ${MODEL_FILE}" 
        break
    elif [ "${STATUS}" == "FAILED" ]
    then 
        echo ""
        echo "FAILURE: Command for model ${MODEL_FILE}"
        exit 1
    else
        sleep 5
    fi           
done


}

generate_data hdfs-parquet datagen-models/weather-model.json 10 100000 10
generate_data hdfs-parquet datagen-models/bank-account-model.json 10 1000000 10
generate_data hdfs-parquet datagen-models/bank-account-model-pr.json 10 1000000 10

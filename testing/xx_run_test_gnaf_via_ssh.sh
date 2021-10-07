
# script runs the pool detection in an EC2 instance via SSH and dumps & downloads the results from the remote Postgres database to a local PG DB

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# load AWS & EC2 vars
. ${GIT_HOME}/temp_ec2_vars.sh

# copy and run python script remotely
FILENAME="test_gdal.py"
scp -F ${SSH_CONFIG} ${SCRIPT_DIR}/${FILENAME} ${USER}@${INSTANCE_ID}:~/
ssh -F ${SSH_CONFIG} ${INSTANCE_ID} "rm ~/test_gdal.log; python3 ${FILENAME}"

# dump results from Postgres and copy locally
ssh -F ${SSH_CONFIG} ${INSTANCE_ID} "pg_dump -Fc -d geo -t bushfire.bal_factors -p 5432 -U ec2-user -f ~/bal_factors.dmp --no-owner"
scp -F ${SSH_CONFIG} ${USER}@${INSTANCE_ID}:~/bal_factors.dmp ${SCRIPT_DIR}/
#scp -F ${SSH_CONFIG} ${USER}@${INSTANCE_ID}:~/test_gdal.log  ${SCRIPT_DIR}/

# load into local postgres (WARNING: force drops tables first)
/Applications/Postgres.app/Contents/Versions/13/bin/psql -d geo -c "drop table bushfire.bal_factors"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres ${SCRIPT_DIR}/bal_factors.dmp

# display table counts
/Applications/Postgres.app/Contents/Versions/13/bin/psql -d geo -c "select count(*) as address_count from bushfire.bal_factors"

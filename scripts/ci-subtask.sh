#!/usr/bin/env bash

# ./ci-subtask.sh <TASK_INDEX>

ROOT_PATH_COV=$(pwd)/covprofile
# Currently, we only have 3 integration tests, so we can hardcode the task index.
integrations_dir=$(pwd)/tests/integrations

case $1 in
    1)
        # unit tests
        ./bin/pd-ut run --race --coverprofile $ROOT_PATH_COV  || exit 1
        ;;
    2)
        # tools tests
        cd ./tools && make ci-test-job && cat covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    3)
        # integration test client
        cd ./client && make ci-test-job && cat covprofile >> $ROOT_PATH_COV  || exit 1
        cd $integrations_dir && make ci-test-job test_name=client && cat ./client/covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    4)
        # integration test tso
        cd $integrations_dir && make ci-test-job test_name=tso && cat ./tso/covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    5)
        # integration test mcs
        cd $integrations_dir && make ci-test-job test_name=mcs && cat ./mcs/covprofile >> $ROOT_PATH_COV || exit 1
        ;;
esac

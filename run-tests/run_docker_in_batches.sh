export test_top_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "test_top_dir is: ${test_top_dir}"

docker run -u root -v ${test_top_dir}:/opt/run-tests hub.tess.io/nudata/nucolumnaraggr:unittest-v0.1.0-release bash -c "cd /opt/run-tests; . ./set_env.sh; ./runtests.sh"


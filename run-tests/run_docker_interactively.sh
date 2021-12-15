export test_top_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "test_top_dir is: ${test_top_dir}"
docker run -v ${test_top_dir}:/opt/run-tests -it --entrypoint=bash hub.tess.io/nudata/nucolumnaraggr:unittest-v0.1.0-release

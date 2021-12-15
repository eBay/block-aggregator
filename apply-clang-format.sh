#!/bin/bash

# Parse args
read -r -d '' USAGE << EOM
apply-clang-format.sh [-v]
    -v validates formatting, returns exit 1 on formatting errors
EOM

while getopts "v" opt; do
    case $opt in
        v)
            VALIDATE=true;;
        *)
            echo "$USAGE"
            exit 1;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1;;
        :)
        echo "Option $OPTARG requires an argument." >&2
        exit 1
    esac
done

find ./src -iname '*.h' -o -iname '*.cpp' -o -iname '*.hpp' -o -iname '*.cc' | xargs clang-format -style=file -i -fallback-style=none

if [ $VALIDATE ]; then
    EXIT_CODE=0
    PATCH_FILE="clang_format.patch"
    git diff > $PATCH_FILE

    # Delete if 0 size
    if [ -s $PATCH_FILE ]
    then
        echo "Code is not according to clang-format-9. Run ./apply-clang-format.sh before committing"
        clang-format --version
        echo "How to install clang-format-9: https://jirap.corp.ebay.com/browse/MONSTOR-11799"
        echo "#### Format Issue:"
        cat $PATCH_FILE
        EXIT_CODE=1
    fi
    rm $PATCH_FILE
    exit $EXIT_CODE
fi

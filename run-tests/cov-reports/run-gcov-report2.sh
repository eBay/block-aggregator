#!/bin/bash

# The combined executable for the corresponding python scripts.
../../binary-distribution/dist/lcov_cobertura/lcov_cobertura  ../../cmake-build-gcov/nucolumnar_aggr_cov.info.cleaned -b ../ \
-e 'deps_build/' \
-e 'deps_prefix/' \
-e 'build/' \
-e 'tests/' \
-e 'test/' \
-e 'usr/' \
-e 'cmake-build-*/' \
-e 'proto/' \
-e 'gen_src/' \
-e 'deps/' -o nucolumnar_aggr_cov.xml

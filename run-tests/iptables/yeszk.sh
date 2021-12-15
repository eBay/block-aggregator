#!/usr/bin/env bash
cat nozk.sh | sed 's/-A/-D/g' | bash


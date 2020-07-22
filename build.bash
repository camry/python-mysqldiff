#!/usr/bin/env bash

pyinstaller -F mysqldiff.py

/bin/cp -f ./dist/mysqldiff ./bin/
/bin/cp -f ./dist/mysqldiff /usr/local/bin/

_MYSQLDIFF_COMPLETE=source /usr/local/bin/mysqldiff > /etc/bash_completion.d/mysqldiff.bash
source /etc/bash_completion.d/mysqldiff.bash

echo "打包完成."

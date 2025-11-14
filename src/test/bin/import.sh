#!/bin/bash

nohup bash import_all_tbl.sh >import.log 2>&1 &
disown
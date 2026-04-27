#!/bin/bash

# Start cdr generator App
nohup python3 Telco_Usecase/cdr_generator/run.py > Telco_Usecase/cdr_generator/output.log 2>&1 &
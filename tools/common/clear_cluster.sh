#!/bin/bash

yarn application -list | egrep '^application_' | awk '{print $1}' | xargs -i -t yarn application -kill {}
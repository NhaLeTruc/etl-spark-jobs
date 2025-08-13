#!/bin/bash

cd src

zip -FSr      ops-core.zip       core -x *.pyc
zip -FSr ops-pipelines.zip  pipelines -x *.pyc

mv      ops-core.zip spark/apps/
mv ops-pipelines.zip spark/apps/
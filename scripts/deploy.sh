#!/bin/bash

cd apps

zip -FSr           ops-zip       core -x *.pyc
zip -FSr ops-pipelines.zip  pipelines -x *.pyc

mv           ops-zip spark/apps/
mv ops-pipelines.zip spark/apps/

#!/bin/bash

## Install Helm
#kubectl create ns flink
#helm repo add bitnami https://charts.bitnami.com/bitnami
#helm repo update

## Install values
helm upgrade -n flink -f flink.yaml flink bitnami/flink

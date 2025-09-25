#!/bin/bash

# create k8s namespace
kubectl create namespace kafka

# install strimzi crd
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# install kafka
kubectl apply -f kraft-single.yml -n kafka

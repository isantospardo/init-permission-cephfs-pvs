package main

import (
        "k8s.io/client-go/kubernetes"
        "k8s.io/client-go/rest"
)

// Declare a kubeclient as a global var to be accessible by all the functions
var kubeclient = Kubeclient{kubeclient: NewKubeClient()}

type Kubeclient struct {
        kubeclient *kubernetes.Clientset
}

// Creates a client
func NewKubeClient() *kubernetes.Clientset {

        // Creates the in-cluster config
        config, err := rest.InClusterConfig()
        if err != nil {
                panic(err.Error())
        }
        // creates the clientset
        clientset, err := kubernetes.NewForConfig(config)
        if err != nil {
                panic(err.Error())
        }
        return clientset
}

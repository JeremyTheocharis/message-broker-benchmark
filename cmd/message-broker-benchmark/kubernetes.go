package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// deletePod deletes the pod with the specified name in the specified namespace.
func deletePod(name, namespace string) error {
	// Create a new Kubernetes client.
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error creating Kubernetes client: %v", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating Kubernetes client: %v", err)
	}

	// Delete the pod.
	if err := client.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("error deleting pod: %v", err)
	}

	return nil
}

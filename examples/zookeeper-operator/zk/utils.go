/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package zk

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// DisableFinalizer disables the finalizers for zookeeper clusters and
// skips the pvc deletion phase when zookeeper cluster get deleted.
var DisableFinalizer bool

const (
	ZkFinalizer = "cleanUpZookeeperPVC"
	ZKMetaRoot  = "/zookeeper-operator"
)

func ContainsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

func RemoveString(slice []string, str string) (result []string) {
	for _, item := range slice {
		if item == str {
			continue
		}
		result = append(result, item)
	}
	return result
}

func IsPVCOrphan(zkPvcName string, replicas int32) bool {
	index := strings.LastIndexAny(zkPvcName, "-")
	if index == -1 {
		return false
	}
	ordinal, err := strconv.Atoi(zkPvcName[index+1:])
	if err != nil {
		return false
	}
	return int32(ordinal) >= replicas
}

func GetZkServiceUri(zoo *ZookeeperCluster) (zkUri string) {
	zkClientPort, _ := ContainerPortByName(zoo.Spec.Ports, "client")
	zkUri = zoo.GetClientServiceName() + "." + zoo.GetNamespace() + ".svc." + zoo.GetKubernetesClusterDomain() + ":" + strconv.Itoa(int(zkClientPort))
	return zkUri
}

func GetMetaPath(zoo *ZookeeperCluster) (path string) {
	return fmt.Sprintf("%s/%s", ZKMetaRoot, zoo.Name)
}

func ContainerPortByName(ports []corev1.ContainerPort, name string) (cPort int32, err error) {
	for _, port := range ports {
		if port.Name == name {
			return port.ContainerPort, nil
		}
	}
	return cPort, fmt.Errorf("port not found")
}

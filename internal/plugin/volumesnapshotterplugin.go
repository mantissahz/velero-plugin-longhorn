/*
Copyright 2018, 2019 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugin

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	vsv1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/volumesnapshotter/v1"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	bsutil "github.com/longhorn/backupstore/util"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

// Volume keeps track of volumes created by this plugin
type Volume struct {
	volName      string
	volAZ        string
	storageClass string
	dataEngine   string
	size         resource.Quantity
}

// Snapshot keeps track of snapshots created by this plugin
type Snapshot struct {
	volName, volAZ string
	tags           map[string]string
}

// VolumeSnapshotter is a plugin for containing state for the blockstore
type VolumeSnapshotter struct {
	config map[string]string
	logrus.FieldLogger
	volumes   map[string]*Volume
	snapshots map[string]*Snapshot

	k8sClient *kubernetes.Clientset
	lhClient  *lhclientset.Clientset
}

// NewVolumeSnapshotter instantiates a VolumeSnapshotter.
func NewVolumeSnapshotter(log logrus.FieldLogger) *VolumeSnapshotter {
	return &VolumeSnapshotter{FieldLogger: log}
}

var _ vsv1.VolumeSnapshotter = (*VolumeSnapshotter)(nil)

// Init prepares the VolumeSnapshotter for usage using the provided map of
// configuration key-value pairs. It returns an error if the VolumeSnapshotter
// cannot be initialized from the provided config. Note that after v0.10.0, this will happen multiple times.
func (p *VolumeSnapshotter) Init(config map[string]string) error {
	p.Infof("Init called", config)
	p.config = config

	// Make sure we don't overwrite data, now that we can re-initialize the plugin
	if p.volumes == nil {
		p.volumes = make(map[string]*Volume)
	}
	if p.snapshots == nil {
		p.snapshots = make(map[string]*Snapshot)
	}

	conf, err := rest.InClusterConfig()
	if err != nil {
		p.Errorf("Failed to get cluster config : %s", err.Error())
		return errors.New("error fetching cluster config")
	}

	k8sClientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		p.Errorf("Error creating k8s clientset : %s", err.Error())
		return errors.Wrap(err, "failed to create k8s client")
	}
	p.k8sClient = k8sClientset

	lhClient, err := lhclientset.NewForConfig(conf)
	if err != nil {
		p.Errorf("Failed to create Longhorn client. %s", err)
		return errors.Wrap(err, "failed to create longhorn client")
	}
	p.lhClient = lhClient

	return nil
}

// CreateVolumeFromSnapshot creates a new volume in the specified
// availability zone, initialized from the provided snapshot,
// and with the specified type and IOPS (if using provisioned IOPS).
func (p *VolumeSnapshotter) CreateVolumeFromSnapshot(snapshotID, volumeType, volumeAZ string, iops *int64) (string, error) {
	p.Infof("CreateVolumeFromSnapshot called", snapshotID, volumeType, volumeAZ, *iops)
	var volumeID string

	return volumeID, nil
}

// GetVolumeInfo returns the type and IOPS (if using provisioned IOPS) for
// the specified volume in the given availability zone.
func (p *VolumeSnapshotter) GetVolumeInfo(volumeID, volumeAZ string) (string, *int64, error) {
	p.Infof("GetVolumeInfo called", volumeID, volumeAZ)
	return "longhorn-volume", nil, nil
}

// IsVolumeReady Check if the volume is ready.
func (p *VolumeSnapshotter) IsVolumeReady(volumeID, volumeAZ string) (ready bool, err error) {
	p.Infof("IsVolumeReady called", volumeID, volumeAZ)
	return true, nil
}

// CreateSnapshot creates a snapshot of the specified volume, and applies any provided
// set of tags to the snapshot.
func (p *VolumeSnapshotter) CreateSnapshot(volumeID, volumeAZ string, tags map[string]string) (string, error) {
	p.Infof("CreateSnapshot called", volumeID, volumeAZ, tags)
	var snapshotID string

	for {
		snapshotID = "velero-" + bsutil.GenerateName("snap")
		p.Infof("CreateSnapshot trying to create snapshot", snapshotID)
		if _, ok := p.snapshots[snapshotID]; ok {
			continue
		}
		break
	}
	p.Infof("Create snapshot %v for volume %v", snapshotID, volumeID)

	// Remember the "original" volume, only required for the first
	// time.
	if _, exists := p.volumes[volumeID]; !exists {
		p.volumes[volumeID] = &Volume{
			volName:    volumeID,
			volAZ:      volumeAZ,
			dataEngine: "v1",
		}
	}

	// Remember the snapshot
	p.snapshots[snapshotID] = &Snapshot{
		volName: volumeID,
		volAZ:   volumeAZ,
		tags:    tags,
	}

	snapshotCR := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotID,
		},
		Spec: longhorn.SnapshotSpec{
			Volume:         volumeID,
			CreateSnapshot: true,
		},
	}

	p.Infof("Creating snapshot %v for volume %v", snapshotID, volumeID)
	_, err := p.lhClient.LonghornV1beta2().Snapshots("longhorn-system").Create(context.TODO(), snapshotCR, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}

	p.Infof("CreateSnapshot returning", snapshotID)
	return snapshotID, nil
}

// DeleteSnapshot deletes the specified volume snapshot.
func (p *VolumeSnapshotter) DeleteSnapshot(snapshotID string) error {
	p.Infof("DeleteSnapshot called", snapshotID)

	return nil
}

// GetVolumeID returns the specific identifier for the PersistentVolume.
func (p *VolumeSnapshotter) GetVolumeID(unstructuredPV runtime.Unstructured) (string, error) {
	p.Infof("GetVolumeID called", unstructuredPV)

	pv := new(v1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPV.UnstructuredContent(), pv); err != nil {
		return "", errors.WithStack(err)
	}

	if pv.Name == "" || pv.Spec.StorageClassName == "" {
		return "", nil
	}

	if _, exists := p.volumes[pv.Name]; !exists {
		p.volumes[pv.Name] = &Volume{
			volName:      pv.Name,
			storageClass: pv.Spec.StorageClassName,
			size:         pv.Spec.Capacity[v1.ResourceStorage],
			dataEngine:   "v1",
		}
	}

	return pv.Name, nil
}

// SetVolumeID sets the specific identifier for the PersistentVolume.
func (p *VolumeSnapshotter) SetVolumeID(unstructuredPV runtime.Unstructured, volumeID string) (runtime.Unstructured, error) {
	p.Infof("SetVolumeID called", unstructuredPV, volumeID)

	pv := new(v1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPV.UnstructuredContent(), pv); err != nil {
		return nil, errors.WithStack(err)
	}

	vol, exists := p.volumes[volumeID]
	if !exists {
		return nil, errors.Errorf("volume %s does not exist in the group", volumeID)
	}

	pv.Name = vol.volName
	res, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pv)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: res}, nil
}

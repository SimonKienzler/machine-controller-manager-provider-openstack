package client

import (
	"fmt"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/metrics"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v2/volumes"
	volumeUtils "github.com/gophercloud/utils/openstack/blockstorage/v2/volumes"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	//VolumeStatusAvailable indicates volume is available
	VolumeStatusAvailable = "available"
	//VolumeStatusCreating indicates volume is creating
	VolumeStatusCreating = "creating"
	//VolumeStatusDownloading indicates volume is downloading
	VolumeStatusDownloading = "downloading"
	//VolumeStatusDeleting indicates volume is deleting
	VolumeStatusDeleting = "deleting"
	//VolumeStatusError indicates volume is in error state
	VolumeStatusError = "error"
)

var (
	_ Storage = &cinderV2{}
)

func newCinderV2(providerClient *gophercloud.ProviderClient, eo gophercloud.EndpointOpts) (*cinderV2, error) {
	storage, err := openstack.NewBlockStorageV2(providerClient, eo)
	if err != nil {
		return nil, fmt.Errorf("could not initialize storage client: %v", err)
	}
	return &cinderV2{
		serviceClient: storage,
	}, nil
}

type cinderV2 struct {
	serviceClient *gophercloud.ServiceClient
}

// GetVolume fetches the volume data from the supplied ID.
func (c cinderV2) GetVolume(id string) (*volumes.Volume, error) {
	volume, err := volumes.Get(c.serviceClient, id).Extract()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil {
		if !IsNotFoundError(err) {
			metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		}
		return nil, err
	}
	return volume, nil
}

// CreateVolume creates a volume.
func (c cinderV2) CreateVolume(opts volumes.CreateOptsBuilder) (*volumes.Volume, error) {
	volume, err := volumes.Create(c.serviceClient, opts).Extract()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return nil, err
	}

	return volume, nil
}

// ListVolumes lists all volumes.
func (c cinderV2) ListVolumes(opts volumes.ListOptsBuilder) ([]volumes.Volume, error) {
	pages, err := volumes.List(c.serviceClient, opts).AllPages()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return nil, err
	}
	return volumes.ExtractVolumes(pages)
}

// UpdateVolume updates the volume from the supplied id.
func (c cinderV2) UpdateVolume(id string, opts volumes.UpdateOptsBuilder) (*volumes.Volume, error) {
	volume, err := volumes.Update(c.serviceClient, id, opts).Extract()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil && !IsNotFoundError(err) {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return nil, err
	}
	return volume, nil
}

// DeleteVolume deletes the volume from the supplied id.
func (c cinderV2) DeleteVolume(id string, opts volumes.DeleteOptsBuilder) error {
	err := volumes.Delete(c.serviceClient, id, opts).ExtractErr()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil && !IsNotFoundError(err) {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return err
	}
	return nil
}

// VolumeIDFromName resolves the given volume name to a unique ID.
func (c cinderV2) VolumeIDFromName(name string) (string, error) {
	return volumeUtils.IDFromName(c.serviceClient, name)
}

package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	objazure "github.com/ankur-anand/objlog/azure"
)

// The well-known Azurite development account. It is public and hard-coded in
// Azurite itself - never a real credential.
const azuriteConnectionString = "DefaultEndpointsProtocol=http;" +
	"AccountName=devstoreaccount1;" +
	"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
	"BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

// openAzurite wires objlog to a local Azurite blob emulator. Swap the
// connection string for a real storage account and nothing else changes.
func openAzurite(ctx context.Context, prefix string) (*provider, error) {
	connectionString := env("OBJLOG_AZURITE_CONNECTION_STRING", azuriteConnectionString)
	containerName := env("OBJLOG_AZURITE_CONTAINER", "objlog-demo")

	client, err := container.NewClientFromConnectionString(connectionString, containerName,
		&container.ClientOptions{
			ClientOptions: policy.ClientOptions{
				PerCallPolicies: []policy.Policy{azuriteVersionPolicy{}},
			},
		})
	if err != nil {
		return nil, fmt.Errorf("azure container client: %w", err)
	}

	if _, err := client.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return nil, fmt.Errorf("create container %q: %w (is Azurite running?)", containerName, err)
	}

	st, err := objazure.New(objazure.Options{
		Container: client,
		Prefix:    prefix,
		StreamID:  streamID,
	})
	if err != nil {
		return nil, fmt.Errorf("azure.New: %w", err)
	}

	list := func(ctx context.Context) ([]object, error) {
		var objects []object
		pager := client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blob := range page.Segment.BlobItems {
				if blob.Name == nil {
					continue
				}
				var size int64
				if blob.Properties != nil && blob.Properties.ContentLength != nil {
					size = *blob.Properties.ContentLength
				}
				objects = append(objects, object{key: *blob.Name, size: size})
			}
		}
		return objects, nil
	}

	return &provider{
		name:        "azurite",
		where:       "http://127.0.0.1:10000/devstoreaccount1",
		container:   "container " + containerName,
		store:       st,
		listObjects: list,
		close:       func() {},
	}, nil
}

// azuriteVersionPolicy pins the REST API version Azurite understands. A real
// storage account does not need this.
type azuriteVersionPolicy struct{}

func (azuriteVersionPolicy) Do(request *policy.Request) (*http.Response, error) {
	delete(request.Raw().Header, "X-Ms-Version")
	request.Raw().Header["x-ms-version"] = []string{"2023-11-03"}
	return request.Next()
}

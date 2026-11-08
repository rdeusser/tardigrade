package model

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// DefaultCatalogURL is the public model catalog.
const DefaultCatalogURL = "https://models.dev/api.json"

// Catalog is public model metadata keyed by provider and model ID.
type Catalog map[string]struct {
	Name   string `json:"name"`
	Models map[string]struct {
		Name  string `json:"name"`
		Limit struct {
			Context int `json:"context"`
			Output  int `json:"output"`
		} `json:"limit"`
	} `json:"models"`
}

// FetchCatalog downloads a catalog in the models.dev format.
func FetchCatalog(ctx context.Context, client *http.Client, url string) (Catalog, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("catalog %s: %s", url, response.Status)
	}
	var catalog Catalog
	return catalog, json.NewDecoder(response.Body).Decode(&catalog)
}

// ContextWindow returns a model's context window in tokens, or zero when the catalog does not list it.
func (c Catalog) ContextWindow(ref Ref) int {
	return c[ref.Provider].Models[ref.ModelID].Limit.Context
}

package interactive

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	"sigs.k8s.io/yaml"
)

type objectCache struct {
	resolver  tracecheck.VersionManager
	jsonCache map[string]string
	gvkCache  map[string]string
	mu        sync.RWMutex
}

func newObjectCache(resolver tracecheck.VersionManager) *objectCache {
	return &objectCache{
		resolver:  resolver,
		jsonCache: make(map[string]string),
		gvkCache:  make(map[string]string),
	}
}

func cacheKeyFor(hash snapshot.VersionHash) string {
	return fmt.Sprintf("%s:%s", hash.Strategy, hash.Value)
}

func (c *objectCache) JSON(hash snapshot.VersionHash) (string, error) {
	if c == nil {
		return "", fmt.Errorf("object cache is not initialized")
	}
	key := cacheKeyFor(hash)
	c.mu.RLock()
	if val, ok := c.jsonCache[key]; ok {
		c.mu.RUnlock()
		return val, nil
	}
	c.mu.RUnlock()

	if c.resolver == nil {
		return "", fmt.Errorf("object resolver is not available")
	}

	obj := c.resolver.Resolve(hash)
	if obj == nil {
		return "", fmt.Errorf("object not found for hash %s (%s)", util.ShortenHash(hash.Value), hash.Strategy)
	}

	data, err := json.Marshal(obj.Object)
	if err != nil {
		return "", err
	}
	jsonStr := string(data)

	c.mu.Lock()
	c.jsonCache[key] = jsonStr
	c.mu.Unlock()

	return jsonStr, nil
}

func (c *objectCache) YAML(hash snapshot.VersionHash) (string, error) {
	jsonStr, err := c.JSON(hash)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(jsonStr) == "" {
		return "(empty)", nil
	}
	out, err := yaml.JSONToYAML([]byte(jsonStr))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (c *objectCache) GVKString(hash snapshot.VersionHash) (string, bool) {
	if c == nil {
		return "", false
	}
	key := cacheKeyFor(hash)

	c.mu.RLock()
	if val, ok := c.gvkCache[key]; ok && val != "" {
		c.mu.RUnlock()
		return val, true
	}
	c.mu.RUnlock()

	if c.resolver == nil {
		return "", false
	}

	obj := c.resolver.Resolve(hash)
	if obj == nil {
		return "", false
	}

	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" {
		gvk.Kind = obj.GetKind()
	}
	if apiVersion := obj.GetAPIVersion(); apiVersion != "" {
		parts := strings.Split(apiVersion, "/")
		switch len(parts) {
		case 2:
			if gvk.Group == "" {
				gvk.Group = parts[0]
			}
			if gvk.Version == "" {
				gvk.Version = parts[1]
			}
		case 1:
			if gvk.Version == "" {
				gvk.Version = parts[0]
			}
		}
	}

	group := gvk.Group
	version := gvk.Version
	kind := gvk.Kind

	if kind == "" {
		kind = obj.GetKind()
	}
	if kind == "" {
		return "", false
	}
	if version == "" {
		version = "?"
	}
	if group == "" {
		if version != "?" {
			group = "core"
		} else {
			group = "?"
		}
	}

	gvkStr := fmt.Sprintf("%s/%s/%s", group, version, kind)

	c.mu.Lock()
	c.gvkCache[key] = gvkStr
	c.mu.Unlock()

	return gvkStr, true
}

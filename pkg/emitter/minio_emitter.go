package emitter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	envMinioEndpoint    = "SLEEVE_MINIO_ENDPOINT"
	envMinioAccessKey   = "SLEEVE_MINIO_ACCESS_KEY"
	envMinioSecretKey   = "SLEEVE_MINIO_SECRET_KEY"
	envMinioBucket      = "SLEEVE_MINIO_BUCKET"
	envMinioUseSSL      = "SLEEVE_MINIO_USE_SSL"
	envMinioCompression = "SLEEVE_MINIO_USE_COMPRESSION"
)

func mustGetEnv(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("environment variable %s not set", key)
	}
	return val, nil
}

func getEnvBool(key string, defaultVal bool) (bool, error) {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal, nil
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("invalid value for %s: %w", key, err)
	}
	return parsed, nil
}

func MinioConfigFromEnv() (MinioConfig, error) {
	endpoint, err := mustGetEnv(envMinioEndpoint)
	if err != nil {
		return MinioConfig{}, err
	}
	accessKey, err := mustGetEnv(envMinioAccessKey)
	if err != nil {
		return MinioConfig{}, err
	}
	secretKey, err := mustGetEnv(envMinioSecretKey)
	if err != nil {
		return MinioConfig{}, err
	}
	bucket, err := mustGetEnv(envMinioBucket)
	if err != nil {
		return MinioConfig{}, err
	}

	useSSL, err := getEnvBool(envMinioUseSSL, false)
	if err != nil {
		return MinioConfig{}, err
	}
	useCompression, err := getEnvBool(envMinioCompression, false)
	if err != nil {
		return MinioConfig{}, err
	}

	skipObjectVersions, _ := getEnvBool("SKIP_OBJECT_VERSIONS", false)
	if skipObjectVersions {
		fmt.Println("SKIP_OBJECT_VERSIONS is set to true, object versions will not be logged")
	}

	return MinioConfig{
		Endpoint:           endpoint,
		AccessKeyID:        accessKey,
		SecretAccessKey:    secretKey,
		BucketName:         bucket,
		UseSSL:             useSSL,
		UseCompression:     useCompression,
		skipObjectVersions: skipObjectVersions,
	}, nil
}

const DefaultBucketName = "sleeve"
const ClusterInternalEndpoint = "minio-svc.sleeve-system.svc.cluster.local:9000"
const ClusterExternalEndpoint = "localhost:9000"

// MinioConfig holds configuration for connecting to a Minio server
type MinioConfig struct {
	Endpoint           string
	AccessKeyID        string
	SecretAccessKey    string
	UseSSL             bool
	BucketName         string
	UseCompression     bool
	skipObjectVersions bool
}

// MinioEmitter implements the Emitter interface to store event data in a Minio bucket
type MinioEmitter struct {
	client *minio.Client
	config MinioConfig
}

func getBucketClient(config MinioConfig) (*minio.Client, error) {
	// Initialize Minio client
	fmt.Println("initializing minio client")
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Minio client: %w", err)
	}

	// Set defaults for paths if not specified

	// Check if bucket exists, create if it doesn't
	exists, err := client.BucketExists(context.Background(), config.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		err = client.MakeBucket(context.Background(), config.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	return client, nil
}

func NewMinioEmitter(config MinioConfig) (*MinioEmitter, error) {
	client, err := getBucketClient(config)
	if err != nil {
		return nil, err
	}
	return &MinioEmitter{
		client: client,
		config: config,
	}, nil
}

func DefaultMinioEmitter() (*MinioEmitter, error) {
	config := MinioConfig{
		Endpoint:        ClusterExternalEndpoint,
		AccessKeyID:     "myaccesskey",
		SecretAccessKey: "mysecretkey",
		UseSSL:          false,
		BucketName:      DefaultBucketName,
		UseCompression:  true,
	}

	return NewMinioEmitter(config)
}

func (m *MinioEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	e, err := event.NewOperation(obj, reconcileID, controllerID, rootID, opType)
	if err != nil {
		// for now, just log it
		fmt.Printf("ERROR: creating event: %v\n", err)
	}
	m.LogOperation(ctx, e)
	if !m.config.skipObjectVersions {
		r, _ := snapshot.AsRecord(obj, reconcileID)
		r.OperationID = e.ID
		r.OperationType = string(opType)
		m.LogObjectVersion(ctx, *r)
	}
}

// LogOperation implements the Emitter interface to store operation events in Minio
func (m *MinioEmitter) LogOperation(ctx context.Context, e *event.Event) {
	eventJSON, err := json.Marshal(e)
	if err != nil {
		// We can't return errors from this interface method, so log and continue
		fmt.Printf("ERROR: failed to serialize event: %v\n", err)
		return
	}

	objectName := path.Join(
		"operations",
		e.ControllerID,
		fmt.Sprintf("%s_%s_%s.json", util.Shorter(e.ReconcileID), e.OpType, e.ID),
	)

	// Upload to Minio
	contentType := "application/json"
	_, err = m.client.PutObject(
		ctx,
		m.config.BucketName,
		objectName,
		bytes.NewReader(eventJSON),
		int64(len(eventJSON)),
		minio.PutObjectOptions{ContentType: contentType},
	)

	if err != nil {
		fmt.Printf("ERROR: failed to upload operation event to Minio: %v\n", err)
	}
}

// LogObjectVersion implements the Emitter interface to store object version records in Minio
func (m *MinioEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	// Serialize record to JSON
	recordJSON, err := json.Marshal(r)
	if err != nil {
		fmt.Printf("ERROR: failed to serialize record: %v\n", err)
		return
	}

	// Create a meaningful object name/path
	timestamp := time.Now().Format(time.RFC3339)
	objectName := path.Join(
		"objects",
		r.Kind,
		r.ObjectID,
		fmt.Sprintf("%s_%s_%s.json", timestamp, r.OperationType, r.OperationID),
	)

	// Upload to Minio
	contentType := "application/json"
	_, err = m.client.PutObject(
		ctx,
		m.config.BucketName,
		objectName,
		bytes.NewReader(recordJSON),
		int64(len(recordJSON)),
		minio.PutObjectOptions{ContentType: contentType},
	)

	if err != nil {
		fmt.Printf("ERROR: failed to upload object version to Minio: %v\n", err)
	}
}

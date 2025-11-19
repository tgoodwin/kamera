package event

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
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
)

const (
	envS3Endpoint    = "KAMERA_S3_ENDPOINT"
	envS3AccessKey   = "KAMERA_S3_ACCESS_KEY"
	envS3SecretKey   = "KAMERA_S3_SECRET_KEY"
	envS3Bucket      = "KAMERA_S3_BUCKET"
	envS3UseSSL      = "KAMERA_S3_USE_SSL"
	envS3Compression = "KAMERA_S3_USE_COMPRESSION"
)

func mustGetEnv(key string) (string, error) {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val, nil
	}
	return "", fmt.Errorf("environment variable %s not set", key)
}

func getEnvBool(key string, defaultVal bool) (bool, error) {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		parsed, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("invalid value for %s: %w", key, err)
		}
		return parsed, nil
	}
	return defaultVal, nil
}

func ObjectStoreConfigFromEnv() (ObjectStoreConfig, error) {
	endpoint, err := mustGetEnv(envS3Endpoint)
	if err != nil {
		return ObjectStoreConfig{}, err
	}
	accessKey, err := mustGetEnv(envS3AccessKey)
	if err != nil {
		return ObjectStoreConfig{}, err
	}
	secretKey, err := mustGetEnv(envS3SecretKey)
	if err != nil {
		return ObjectStoreConfig{}, err
	}
	bucket, err := mustGetEnv(envS3Bucket)
	if err != nil {
		return ObjectStoreConfig{}, err
	}

	useSSL, err := getEnvBool(envS3UseSSL, false)
	if err != nil {
		return ObjectStoreConfig{}, err
	}
	useCompression, err := getEnvBool(envS3Compression, false)
	if err != nil {
		return ObjectStoreConfig{}, err
	}

	return ObjectStoreConfig{
		Endpoint:        endpoint,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		BucketName:      bucket,
		UseSSL:          useSSL,
		UseCompression:  useCompression,
	}, nil
}

const DefaultBucketName = "sleeve"
const ClusterInternalEndpoint = "minio-svc.sleeve-system.svc.cluster.local:9000"
const ClusterExternalEndpoint = "localhost:9000"

// ObjectStoreEmitter implements the Emitter interface to store event data in an S3-compatible bucket.
type ObjectStoreEmitter struct {
	client         *minio.Client
	bucketName     string
	useCompression bool
}

// ObjectStoreConfig holds configuration for connecting to an S3-compatible object store.
type ObjectStoreConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
	UseCompression  bool
}

func getBucketClient(config ObjectStoreConfig) (*minio.Client, error) {
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

func NewObjectStoreEmitter(config ObjectStoreConfig) (*ObjectStoreEmitter, error) {
	client, err := getBucketClient(config)
	if err != nil {
		return nil, err
	}
	return &ObjectStoreEmitter{
		client:         client,
		bucketName:     config.BucketName,
		useCompression: config.UseCompression,
	}, nil
}

func DefaultObjectStoreEmitter() (*ObjectStoreEmitter, error) {
	config := ObjectStoreConfig{
		Endpoint:        ClusterExternalEndpoint,
		AccessKeyID:     "myaccesskey",
		SecretAccessKey: "mysecretkey",
		UseSSL:          false,
		BucketName:      DefaultBucketName,
		UseCompression:  true,
	}

	return NewObjectStoreEmitter(config)
}

// LogOperation implements the Emitter interface to store operation events.
func (m *ObjectStoreEmitter) LogOperation(ctx context.Context, e *Event) {
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

	// Upload to the configured object store
	contentType := "application/json"
	_, err = m.client.PutObject(
		ctx,
		m.bucketName,
		objectName,
		bytes.NewReader(eventJSON),
		int64(len(eventJSON)),
		minio.PutObjectOptions{ContentType: contentType},
	)

	if err != nil {
		fmt.Printf("ERROR: failed to upload operation event to object store: %v\n", err)
	}
}

// LogObjectVersion implements the Emitter interface to store object version records.
func (m *ObjectStoreEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
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

	// Upload to the configured object store
	contentType := "application/json"
	_, err = m.client.PutObject(
		ctx,
		m.bucketName,
		objectName,
		bytes.NewReader(recordJSON),
		int64(len(recordJSON)),
		minio.PutObjectOptions{ContentType: contentType},
	)

	if err != nil {
		fmt.Printf("ERROR: failed to upload object version to object store: %v\n", err)
	}
}

type MinioEmitter = ObjectStoreEmitter
type MinioConfig = ObjectStoreConfig

func MinioConfigFromEnv() (MinioConfig, error) {
	return ObjectStoreConfigFromEnv()
}

func NewMinioEmitter(config MinioConfig) (*MinioEmitter, error) {
	return NewObjectStoreEmitter(config)
}

func DefaultMinioEmitter() (*MinioEmitter, error) {
	return DefaultObjectStoreEmitter()
}

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
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
)

// MinioEmitter implements the Emitter interface to store event data in a Minio bucket
type MinioEmitter struct {
	client         *minio.Client
	bucketName     string
	useCompression bool
}

// MinioConfig holds configuration for connecting to a Minio server
type MinioConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
	UseCompression  bool
}

func GetBucketClient(config MinioConfig) (*minio.Client, error) {
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
	client, err := GetBucketClient(config)
	if err != nil {
		return nil, err
	}
	return &MinioEmitter{
		client:         client,
		bucketName:     config.BucketName,
		useCompression: config.UseCompression,
	}, nil
}

func DefaultMinioEmitter() (*MinioEmitter, error) {
	config := MinioConfig{
		Endpoint:        "localhost:9000",
		AccessKeyID:     "myaccesskey",
		SecretAccessKey: "mysecretkey",
		UseSSL:          false,
		BucketName:      "sleeve3",
		UseCompression:  true,
	}

	return NewMinioEmitter(config)
}

// LogOperation implements the Emitter interface to store operation events in Minio
func (m *MinioEmitter) LogOperation(ctx context.Context, e *Event) {
	// Serialize event to JSON
	eventJSON, err := json.Marshal(e)
	if err != nil {
		// We can't return errors from this interface method, so log and continue
		fmt.Printf("ERROR: failed to serialize event: %v\n", err)
		return
	}

	// Create a meaningful object name/path
	// timestamp := time.Now().Format(time.RFC3339)
	// for example, "operations/controllerID/reconcileID/2021-08-01T12:00:00Z_create_123.json"
	objectName := path.Join(
		"operations",
		e.ControllerID,
		// e.ReconcileID,
		fmt.Sprintf("%s_%s_%s.json", util.Shorter(e.ReconcileID), e.OpType, e.ID),
	)

	// Upload to Minio
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
		m.bucketName,
		objectName,
		bytes.NewReader(recordJSON),
		int64(len(recordJSON)),
		minio.PutObjectOptions{ContentType: contentType},
	)

	if err != nil {
		fmt.Printf("ERROR: failed to upload object version to Minio: %v\n", err)
	}
}

// K8sMinioEnvConfig holds the environment variable names for Minio configuration
type K8sMinioEnvConfig struct {
	EndpointEnv        string
	AccessKeyIDEnv     string
	SecretAccessKeyEnv string
	UseSSLEnv          string
	BucketNameEnv      string
	OperationsPathEnv  string
	ObjectsPathEnv     string
	UseCompressionEnv  string
}

// DefaultK8sMinioConfig returns a K8sMinioConfig with default environment variable names
func DefaultK8sMinioConfig() K8sMinioEnvConfig {
	return K8sMinioEnvConfig{
		EndpointEnv:        "SLEEVE_MINIO_ENDPOINT",
		AccessKeyIDEnv:     "SLEEVE_MINIO_ACCESS_KEY",
		SecretAccessKeyEnv: "SLEEVE_MINIO_SECRET_KEY",
		UseSSLEnv:          "SLEEVE_MINIO_USE_SSL",
		BucketNameEnv:      "SLEEVE_MINIO_BUCKET",
		OperationsPathEnv:  "SLEEVE_MINIO_OPERATIONS_PATH",
		ObjectsPathEnv:     "SLEEVE_MINIO_OBJECTS_PATH",
		UseCompressionEnv:  "SLEEVE_MINIO_USE_COMPRESSION",
	}
}

// NewMinioEmitterFromEnv creates a new MinioEmitter using configuration from environment variables
func NewMinioEmitterFromEnv() (*MinioEmitter, error) {
	return NewMinioEmitterFromK8sConfig(DefaultK8sMinioConfig())
}

// NewMinioEmitterFromK8sConfig creates a new MinioEmitter using the provided K8sMinioConfig
func NewMinioEmitterFromK8sConfig(config K8sMinioEnvConfig) (*MinioEmitter, error) {
	// Get environment variables
	endpoint := os.Getenv(config.EndpointEnv)
	accessKeyID := os.Getenv(config.AccessKeyIDEnv)
	secretAccessKey := os.Getenv(config.SecretAccessKeyEnv)
	useSSLStr := os.Getenv(config.UseSSLEnv)
	bucketName := os.Getenv(config.BucketNameEnv)
	useCompressionStr := os.Getenv(config.UseCompressionEnv)

	// Validate required fields
	if endpoint == "" {
		return nil, fmt.Errorf("minio endpoint not provided, set %s", config.EndpointEnv)
	}
	if accessKeyID == "" {
		return nil, fmt.Errorf("minio access key ID not provided, set %s", config.AccessKeyIDEnv)
	}
	if secretAccessKey == "" {
		return nil, fmt.Errorf("minio secret access key not provided, set %s", config.SecretAccessKeyEnv)
	}
	if bucketName == "" {
		return nil, fmt.Errorf("minio bucket name not provided, set %s", config.BucketNameEnv)
	}

	// Parse boolean values
	useSSL := false
	if useSSLStr != "" {
		var err error
		useSSL, err = strconv.ParseBool(useSSLStr)
		if err != nil {
			return nil, fmt.Errorf("invalid value for %s: %w", config.UseSSLEnv, err)
		}
	}

	useCompression := false
	if useCompressionStr != "" {
		var err error
		useCompression, err = strconv.ParseBool(useCompressionStr)
		if err != nil {
			return nil, fmt.Errorf("invalid value for %s: %w", config.UseCompressionEnv, err)
		}
	}

	// Create MinioConfig
	minioConfig := MinioConfig{
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UseSSL:          useSSL,
		BucketName:      bucketName,
		UseCompression:  useCompression,
	}

	// Create MinioEmitter
	return NewMinioEmitter(minioConfig)
}

package file

import (
	"fmt"
	"os"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// DefaultProviderFromEnv returns the platform storage provider from STORAGE_TYPE.
func DefaultProviderFromEnv() string {
	p := strings.ToLower(strings.TrimSpace(os.Getenv("STORAGE_TYPE")))
	if p == "" {
		return "local"
	}
	return p
}

// StorageEngineConfigFromEnv builds tenant-style storage config purely from environment variables.
func StorageEngineConfigFromEnv() *types.StorageEngineConfig {
	provider := DefaultProviderFromEnv()
	cfg := &types.StorageEngineConfig{DefaultProvider: provider}

	switch provider {
	case "local":
		cfg.Local = &types.LocalEngineConfig{
			PathPrefix: strings.TrimSpace(os.Getenv("LOCAL_STORAGE_PATH_PREFIX")),
		}
	case "minio":
		cfg.MinIO = &types.MinIOEngineConfig{
			Mode:            "docker",
			Endpoint:        strings.TrimSpace(os.Getenv("MINIO_ENDPOINT")),
			AccessKeyID:     strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY_ID")),
			SecretAccessKey: strings.TrimSpace(os.Getenv("MINIO_SECRET_ACCESS_KEY")),
			BucketName:      strings.TrimSpace(os.Getenv("MINIO_BUCKET_NAME")),
			UseSSL:          strings.EqualFold(os.Getenv("MINIO_USE_SSL"), "true"),
			PathPrefix:      strings.TrimSpace(os.Getenv("MINIO_PATH_PREFIX")),
		}
	case "cos":
		cfg.COS = &types.COSEngineConfig{
			SecretID:   strings.TrimSpace(os.Getenv("COS_SECRET_ID")),
			SecretKey:  strings.TrimSpace(os.Getenv("COS_SECRET_KEY")),
			Region:     strings.TrimSpace(os.Getenv("COS_REGION")),
			BucketName: strings.TrimSpace(os.Getenv("COS_BUCKET_NAME")),
			AppID:      strings.TrimSpace(os.Getenv("COS_APP_ID")),
			PathPrefix: strings.TrimSpace(os.Getenv("COS_PATH_PREFIX")),
		}
	case "tos":
		cfg.TOS = &types.TOSEngineConfig{
			Endpoint:   strings.TrimSpace(os.Getenv("TOS_ENDPOINT")),
			Region:     strings.TrimSpace(os.Getenv("TOS_REGION")),
			AccessKey:  strings.TrimSpace(os.Getenv("TOS_ACCESS_KEY")),
			SecretKey:  strings.TrimSpace(os.Getenv("TOS_SECRET_KEY")),
			BucketName: strings.TrimSpace(os.Getenv("TOS_BUCKET_NAME")),
			PathPrefix: strings.TrimSpace(os.Getenv("TOS_PATH_PREFIX")),
		}
	case "s3":
		pathPrefix := strings.TrimSpace(os.Getenv("S3_PATH_PREFIX"))
		if pathPrefix == "" {
			pathPrefix = "weknora/"
		}
		cfg.S3 = &types.S3EngineConfig{
			Endpoint:   strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
			Region:     strings.TrimSpace(os.Getenv("S3_REGION")),
			AccessKey:  strings.TrimSpace(os.Getenv("S3_ACCESS_KEY")),
			SecretKey:  strings.TrimSpace(os.Getenv("S3_SECRET_KEY")),
			BucketName: strings.TrimSpace(os.Getenv("S3_BUCKET_NAME")),
			PathPrefix: pathPrefix,
		}
	case "oss":
		pathPrefix := strings.TrimSpace(os.Getenv("OSS_PATH_PREFIX"))
		if pathPrefix == "" {
			pathPrefix = "weknora/"
		}
		cfg.OSS = &types.OSSEngineConfig{
			Endpoint:       strings.TrimSpace(os.Getenv("OSS_ENDPOINT")),
			Region:         strings.TrimSpace(os.Getenv("OSS_REGION")),
			AccessKey:      strings.TrimSpace(os.Getenv("OSS_ACCESS_KEY")),
			SecretKey:      strings.TrimSpace(os.Getenv("OSS_SECRET_KEY")),
			BucketName:     strings.TrimSpace(os.Getenv("OSS_BUCKET_NAME")),
			PathPrefix:     pathPrefix,
			UseTempBucket:  strings.TrimSpace(os.Getenv("OSS_TEMP_BUCKET_NAME")) != "",
			TempBucketName: strings.TrimSpace(os.Getenv("OSS_TEMP_BUCKET_NAME")),
			TempRegion:     strings.TrimSpace(os.Getenv("OSS_TEMP_REGION")),
		}
	case "ks3":
		pathPrefix := strings.TrimSpace(os.Getenv("KS3_PATH_PREFIX"))
		if pathPrefix == "" {
			pathPrefix = "weknora/"
		}
		cfg.KS3 = &types.KS3EngineConfig{
			Endpoint:   strings.TrimSpace(os.Getenv("KS3_ENDPOINT")),
			Region:     strings.TrimSpace(os.Getenv("KS3_REGION")),
			AccessKey:  strings.TrimSpace(os.Getenv("KS3_ACCESS_KEY")),
			SecretKey:  strings.TrimSpace(os.Getenv("KS3_SECRET_KEY")),
			BucketName: strings.TrimSpace(os.Getenv("KS3_BUCKET_NAME")),
			PathPrefix: pathPrefix,
		}
	}

	return cfg
}

// IsProviderConfiguredFromEnv reports whether env vars for the given provider are complete.
func IsProviderConfiguredFromEnv(provider string) bool {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = DefaultProviderFromEnv()
	}
	switch provider {
	case "local":
		return true
	case "dummy":
		return true
	case "minio":
		return os.Getenv("MINIO_ENDPOINT") != "" &&
			os.Getenv("MINIO_ACCESS_KEY_ID") != "" &&
			os.Getenv("MINIO_SECRET_ACCESS_KEY") != "" &&
			os.Getenv("MINIO_BUCKET_NAME") != ""
	case "cos":
		return os.Getenv("COS_BUCKET_NAME") != "" &&
			os.Getenv("COS_REGION") != "" &&
			os.Getenv("COS_SECRET_ID") != "" &&
			os.Getenv("COS_SECRET_KEY") != "" &&
			os.Getenv("COS_PATH_PREFIX") != ""
	case "tos":
		return os.Getenv("TOS_ENDPOINT") != "" &&
			os.Getenv("TOS_REGION") != "" &&
			os.Getenv("TOS_ACCESS_KEY") != "" &&
			os.Getenv("TOS_SECRET_KEY") != "" &&
			os.Getenv("TOS_BUCKET_NAME") != ""
	case "s3":
		return os.Getenv("S3_ENDPOINT") != "" &&
			os.Getenv("S3_REGION") != "" &&
			os.Getenv("S3_ACCESS_KEY") != "" &&
			os.Getenv("S3_SECRET_KEY") != "" &&
			os.Getenv("S3_BUCKET_NAME") != ""
	case "oss":
		return os.Getenv("OSS_ENDPOINT") != "" &&
			os.Getenv("OSS_REGION") != "" &&
			os.Getenv("OSS_ACCESS_KEY") != "" &&
			os.Getenv("OSS_SECRET_KEY") != "" &&
			os.Getenv("OSS_BUCKET_NAME") != ""
	case "ks3":
		return os.Getenv("KS3_ENDPOINT") != "" &&
			os.Getenv("KS3_REGION") != "" &&
			os.Getenv("KS3_ACCESS_KEY") != "" &&
			os.Getenv("KS3_SECRET_KEY") != "" &&
			os.Getenv("KS3_BUCKET_NAME") != ""
	default:
		return false
	}
}

// IsEnvStorageConfigured reports whether the active STORAGE_TYPE env configuration is usable.
func IsEnvStorageConfigured() bool {
	return IsProviderConfiguredFromEnv(DefaultProviderFromEnv())
}

// NewFileServiceFromEnv creates the platform FileService from environment variables.
func NewFileServiceFromEnv() (interfaces.FileService, string, error) {
	provider := DefaultProviderFromEnv()
	sec := StorageEngineConfigFromEnv()
	return NewFileServiceFromStorageConfig(provider, sec, localBaseDirFromEnv())
}

func localBaseDirFromEnv() string {
	baseDir := strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR"))
	if baseDir == "" {
		baseDir = "/data/files"
	}
	return baseDir
}

// DocParserStorageConfigFromEnv builds docreader storage config from environment variables.
func DocParserStorageConfigFromEnv() *types.DocParserStorageConfig {
	provider := DefaultProviderFromEnv()
	return buildDocParserStorageConfigFromEngineConfig(provider, StorageEngineConfigFromEnv())
}

func buildDocParserStorageConfigFromEngineConfig(provider string, sec *types.StorageEngineConfig) *types.DocParserStorageConfig {
	out := &types.DocParserStorageConfig{Provider: strings.ToUpper(provider)}
	if sec == nil {
		return out
	}
	switch provider {
	case "local":
		if sec.Local != nil {
			out.PathPrefix = sec.Local.PathPrefix
		}
	case "minio":
		if sec.MinIO != nil {
			out.Endpoint = sec.MinIO.Endpoint
			out.AccessKeyID = sec.MinIO.AccessKeyID
			out.SecretAccessKey = sec.MinIO.SecretAccessKey
			out.BucketName = sec.MinIO.BucketName
			out.PathPrefix = sec.MinIO.PathPrefix
		}
	case "cos":
		if sec.COS != nil {
			out.Region = sec.COS.Region
			out.BucketName = sec.COS.BucketName
			out.AccessKeyID = sec.COS.SecretID
			out.SecretAccessKey = sec.COS.SecretKey
			out.AppID = sec.COS.AppID
			out.PathPrefix = sec.COS.PathPrefix
		}
	case "tos":
		if sec.TOS != nil {
			out.Endpoint = sec.TOS.Endpoint
			out.Region = sec.TOS.Region
			out.AccessKeyID = sec.TOS.AccessKey
			out.SecretAccessKey = sec.TOS.SecretKey
			out.BucketName = sec.TOS.BucketName
			out.PathPrefix = sec.TOS.PathPrefix
		}
	case "s3":
		if sec.S3 != nil {
			out.Endpoint = sec.S3.Endpoint
			out.Region = sec.S3.Region
			out.AccessKeyID = sec.S3.AccessKey
			out.SecretAccessKey = sec.S3.SecretKey
			out.BucketName = sec.S3.BucketName
			out.PathPrefix = sec.S3.PathPrefix
		}
	case "oss":
		if sec.OSS != nil {
			out.Endpoint = sec.OSS.Endpoint
			out.Region = sec.OSS.Region
			out.AccessKeyID = sec.OSS.AccessKey
			out.SecretAccessKey = sec.OSS.SecretKey
			out.BucketName = sec.OSS.BucketName
			out.PathPrefix = sec.OSS.PathPrefix
		}
	case "ks3":
		if sec.KS3 != nil {
			out.Endpoint = sec.KS3.Endpoint
			out.Region = sec.KS3.Region
			out.AccessKeyID = sec.KS3.AccessKey
			out.SecretAccessKey = sec.KS3.SecretKey
			out.BucketName = sec.KS3.BucketName
			out.PathPrefix = sec.KS3.PathPrefix
		}
	}
	return out
}

// EnvStorageInitError returns an initialization error for the active provider, if any.
func EnvStorageInitError() error {
	if IsEnvStorageConfigured() {
		return nil
	}
	return fmt.Errorf("storage engine is not configured in environment (STORAGE_TYPE=%s)", DefaultProviderFromEnv())
}

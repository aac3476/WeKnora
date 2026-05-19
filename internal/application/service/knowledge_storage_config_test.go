package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

// TestBuildStorageConfig_FromEnv verifies buildStorageConfig always reads platform
// storage settings from environment variables, ignoring tenant/KB overrides.
func TestBuildStorageConfig_FromEnv(t *testing.T) {

	tests := []struct {
		name string
		env  map[string]string
		want struct {
			provider        string
			region          string
			bucket          string
			accessKeyID     string
			secretAccessKey string
			appID           string
			pathPrefix      string
			endpoint        string
		}
	}{
		{
			name: "local",
			env: map[string]string{
				"STORAGE_TYPE":               "local",
				"LOCAL_STORAGE_PATH_PREFIX":  "/data/wk",
			},
			want: struct {
				provider        string
				region          string
				bucket          string
				accessKeyID     string
				secretAccessKey string
				appID           string
				pathPrefix      string
				endpoint        string
			}{provider: "LOCAL", pathPrefix: "/data/wk"},
		},
		{
			name: "oss",
			env: map[string]string{
				"STORAGE_TYPE":     "oss",
				"OSS_ENDPOINT":     "oss-cn-hangzhou.aliyuncs.com",
				"OSS_REGION":       "cn-hangzhou",
				"OSS_ACCESS_KEY":   "oss-ak",
				"OSS_SECRET_KEY":   "oss-sk",
				"OSS_BUCKET_NAME":  "kb-oss",
				"OSS_PATH_PREFIX":  "wk/",
			},
			want: struct {
				provider        string
				region          string
				bucket          string
				accessKeyID     string
				secretAccessKey string
				appID           string
				pathPrefix      string
				endpoint        string
			}{
				provider: "OSS", endpoint: "oss-cn-hangzhou.aliyuncs.com", region: "cn-hangzhou",
				accessKeyID: "oss-ak", secretAccessKey: "oss-sk",
				bucket: "kb-oss", pathPrefix: "wk/",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			// Tenant config must be ignored after the env-only migration.
			tenant := &types.Tenant{StorageEngineConfig: &types.StorageEngineConfig{
				OSS: &types.OSSEngineConfig{
					Endpoint: "ignored.example.com", Region: "ignored",
					AccessKey: "ignored", SecretKey: "ignored", BucketName: "ignored",
				},
			}}
			kb := &types.KnowledgeBase{
				ID:                    "kb-env-" + tt.name,
				StorageProviderConfig: &types.StorageProviderConfig{Provider: "oss"},
			}
			ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, tenant)

			s := &knowledgeService{}
			got := s.buildStorageConfig(ctx, kb)
			if got == nil {
				t.Fatalf("buildStorageConfig returned nil")
			}
			if got.Provider != tt.want.provider {
				t.Errorf("Provider = %q, want %q", got.Provider, tt.want.provider)
			}
			if got.Region != tt.want.region {
				t.Errorf("Region = %q, want %q", got.Region, tt.want.region)
			}
			if got.BucketName != tt.want.bucket {
				t.Errorf("BucketName = %q, want %q", got.BucketName, tt.want.bucket)
			}
			if got.AccessKeyID != tt.want.accessKeyID {
				t.Errorf("AccessKeyID = %q, want %q", got.AccessKeyID, tt.want.accessKeyID)
			}
			if got.SecretAccessKey != tt.want.secretAccessKey {
				t.Errorf("SecretAccessKey = %q, want %q", got.SecretAccessKey, tt.want.secretAccessKey)
			}
			if got.AppID != tt.want.appID {
				t.Errorf("AppID = %q, want %q", got.AppID, tt.want.appID)
			}
			if got.PathPrefix != tt.want.pathPrefix {
				t.Errorf("PathPrefix = %q, want %q", got.PathPrefix, tt.want.pathPrefix)
			}
			if got.Endpoint != tt.want.endpoint {
				t.Errorf("Endpoint = %q, want %q", got.Endpoint, tt.want.endpoint)
			}
		})
	}
}

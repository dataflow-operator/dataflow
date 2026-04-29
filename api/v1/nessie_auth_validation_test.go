package v1

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateNessieAuthConfig(t *testing.T) {
	tests := []struct {
		name     string
		authType string
		token    string
		basic    *BasicAuthConfig
		wantErr  bool
	}{
		{
			name:     "auto without credentials is allowed",
			authType: "",
			wantErr:  false,
		},
		{
			name:     "bearer requires token",
			authType: "BEARER",
			wantErr:  true,
		},
		{
			name:     "bearer with token is valid",
			authType: "BEARER",
			token:    "tok",
			wantErr:  false,
		},
		{
			name:     "basic requires basic auth",
			authType: "BASIC",
			wantErr:  true,
		},
		{
			name:     "basic with credentials is valid",
			authType: "BASIC",
			basic: &BasicAuthConfig{
				Username: "alice",
				Password: "secret",
			},
			wantErr: false,
		},
		{
			name:     "none allows empty",
			authType: "NONE",
			wantErr:  false,
		},
		{
			name:     "unknown value is rejected",
			authType: "JWT",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validateNessieAuthConfig(tt.authType, tt.token, nil, tt.basic, field.NewPath("config"))
			if tt.wantErr && len(errs) == 0 {
				t.Fatalf("expected validation errors, got none")
			}
			if !tt.wantErr && len(errs) > 0 {
				t.Fatalf("expected no validation errors, got %v", errs)
			}
		})
	}
}

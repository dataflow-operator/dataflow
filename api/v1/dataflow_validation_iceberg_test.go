package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateIcebergSource(t *testing.T) {
	valid := &IcebergSourceSpec{
		CatalogURI: "https://catalog:8181",
		Namespace:  "ns",
		Table:      "t",
	}
	assert.Empty(t, validateIcebergSource(valid, field.NewPath("config")))

	missing := &IcebergSourceSpec{Namespace: "ns", Table: "t"}
	errs := validateIcebergSource(missing, field.NewPath("config"))
	require.NotEmpty(t, errs)

	inc := true
	withQuery := &IcebergSourceSpec{
		CatalogURI:            "https://catalog:8181",
		Namespace:             "ns",
		Table:                 "t",
		Query:                 "SELECT 1",
		IncrementalBySnapshot: &inc,
	}
	errs = validateIcebergSource(withQuery, field.NewPath("config"))
	require.NotEmpty(t, errs)
}

func TestValidateIcebergSink(t *testing.T) {
	valid := &IcebergSinkSpec{
		CatalogURI: "https://catalog:8181",
		Namespace:  "ns",
		Table:      "t",
	}
	assert.Empty(t, validateIcebergSink(valid, field.NewPath("config")))

	conflict := &IcebergSinkSpec{
		CatalogURI:         "https://catalog:8181",
		Namespace:          "ns",
		Table:              "t",
		BearerToken:        "tok",
		OAuth2ClientID:     "id",
		OAuth2ClientSecret: "sec",
	}
	errs := validateIcebergSink(conflict, field.NewPath("config"))
	require.NotEmpty(t, errs)
}

func TestValidateIcebergRESTAuthConfig(t *testing.T) {
	errs := validateIcebergRESTAuthConfig(
		string(IcebergRESTAuthenticationBearer),
		"tok",
		nil,
		nil,
		"id",
		nil,
		"sec",
		nil,
		field.NewPath("config"),
	)
	require.NotEmpty(t, errs)
}

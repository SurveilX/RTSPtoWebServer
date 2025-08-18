package main

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

type EnvConfig struct {
	DOSpacesAccessKey string
	DOSpacesSecretKey string
	DOSpacesEndpoint  string
	DOSpacesRegion    string
	DOSpacesBucket    string

	DeleteLocalAfterUpload bool

	HTTPPort string
	RTSPPort string
	Debug    bool
}

var envConfig *EnvConfig

// LoadEnvConfig loads configuration from .env file
func LoadEnvConfig() {
	if err := godotenv.Load(); err != nil {
		configDir := filepath.Dir(configFile)
		envPath := filepath.Join(configDir, ".env")
		if err := godotenv.Load(envPath); err != nil {
			log.WithFields(logrus.Fields{
				"module": "env_config",
				"func":   "LoadEnvConfig",
			}).Warnln("No .env file found, using system environment variables")
		} else {
			log.WithFields(logrus.Fields{
				"module": "env_config",
				"func":   "LoadEnvConfig",
				"path":   envPath,
			}).Infoln("Loaded environment variables from .env file")
		}
	} else {
		log.WithFields(logrus.Fields{
			"module": "env_config",
			"func":   "LoadEnvConfig",
		}).Infoln("Loaded environment variables from .env file")
	}

	envConfig = &EnvConfig{
		DOSpacesAccessKey: getEnv("DO_SPACES_ACCESS_KEY", ""),
		DOSpacesSecretKey: getEnv("DO_SPACES_SECRET_KEY", ""),
		DOSpacesEndpoint:  getEnv("DO_SPACES_ENDPOINT", ""),
		DOSpacesRegion:    getEnv("DO_SPACES_REGION", ""),
		DOSpacesBucket:    getEnv("DO_SPACES_BUCKET", ""),
		DeleteLocalAfterUpload: getEnvBool("DELETE_LOCAL_AFTER_UPLOAD", true),
		HTTPPort: getEnv("HTTP_PORT", ""),
		RTSPPort: getEnv("RTSP_PORT", ""),
		Debug:    getEnvBool("DEBUG", false),
	}

	log.WithFields(logrus.Fields{
		"module": "env_config",
		"func":   "LoadEnvConfig",
		"do_endpoint": envConfig.DOSpacesEndpoint,
		"do_region":   envConfig.DOSpacesRegion,
		"do_bucket":   envConfig.DOSpacesBucket,
		"do_configured": envConfig.DOSpacesAccessKey != "" && envConfig.DOSpacesSecretKey != "",
		"delete_local": envConfig.DeleteLocalAfterUpload,
	}).Infoln("Environment configuration loaded")

	if envConfig.DOSpacesAccessKey == "" || envConfig.DOSpacesSecretKey == "" {
		log.WithFields(logrus.Fields{
			"module": "env_config",
			"func":   "LoadEnvConfig",
		}).Warnln("Digital Ocean Spaces credentials not configured - uploads will be skipped")
	}

	if envConfig.DOSpacesBucket == "" {
		log.WithFields(logrus.Fields{
			"module": "env_config",
			"func":   "LoadEnvConfig",
		}).Warnln("Digital Ocean Spaces bucket not configured - uploads will be skipped")
	}
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool gets a boolean environment variable with a default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// GetEnvConfig returns the loaded environment configuration
func GetEnvConfig() *EnvConfig {
	if envConfig == nil {
		LoadEnvConfig()
	}
	return envConfig
}

// IsDigitalOceanConfigured checks if Digital Ocean Spaces is properly configured
func IsDigitalOceanConfigured() bool {
	config := GetEnvConfig()
	return config.DOSpacesAccessKey != "" && 
		   config.DOSpacesSecretKey != "" && 
		   config.DOSpacesEndpoint != "" && 
		   config.DOSpacesBucket != ""
}

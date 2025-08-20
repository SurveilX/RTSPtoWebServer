package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sirupsen/logrus"
)

// Digital Ocean Spaces configuration
type DOConfig struct {
	AccessKey string
	SecretKey string
	Endpoint  string
	Region    string
	Bucket    string
}

// getDOConfigFromEnv reads Digital Ocean configuration from .env file
func getDOConfigFromEnv() *DOConfig {
	envConfig := GetEnvConfig()
	return &DOConfig{
		AccessKey: envConfig.DOSpacesAccessKey,
		SecretKey: envConfig.DOSpacesSecretKey,
		Endpoint:  envConfig.DOSpacesEndpoint,
		Region:    envConfig.DOSpacesRegion,
		Bucket:    envConfig.DOSpacesBucket,
	}
}

// uploadVSSToDigitalOcean uploads VSS recording and poster to Digital Ocean Spaces
func uploadVSSToDigitalOcean(session *RecordingSession) (string, string, error) {
	doConfig := getDOConfigFromEnv()

	if !IsDigitalOceanConfigured() {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
		}).Warnln("Digital Ocean Spaces not configured in .env file, skipping upload")
		return "", "", nil
	}

	if _, err := os.Stat(session.FilePath); os.IsNotExist(err) {
		return "", "", fmt.Errorf("video file does not exist: %s", session.FilePath)
	}

	session.mutex.Lock()
	session.Status = "uploading"
	session.mutex.Unlock()

	log.WithFields(logrus.Fields{
		"module":        "recording",
		"session_id":    session.ID,
		"video_path":    session.FilePath,
		"poster_path":   session.PosterPath,
		"incident_type": session.IncidentType,
		"store_id":      session.StoreID,
		"bucket":        doConfig.Bucket,
		"endpoint":      doConfig.Endpoint,
	}).Infoln("Starting VSS upload to Digital Ocean Spaces")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			doConfig.AccessKey,
			doConfig.SecretKey,
			"",
		)),
		config.WithRegion(doConfig.Region),
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("https://" + doConfig.Endpoint)
		o.UsePathStyle = false
	})

	clipURL, err := uploadVSSFile(client, doConfig.Bucket, session.FilePath, "video/mp4", session)
	if err != nil {
		session.mutex.Lock()
		session.Status = "upload_failed"
		session.mutex.Unlock()
		return "", "", fmt.Errorf("failed to upload video: %v", err)
	}

	var posterURL string
	if _, err := os.Stat(session.PosterPath); err == nil {
		posterURL, err = uploadVSSFile(client, doConfig.Bucket, session.PosterPath, "image/jpeg", session)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Warnln("Failed to upload poster, continuing with video only")
		}
	}

	session.mutex.Lock()
	session.Status = "uploaded"
	session.mutex.Unlock()

	log.WithFields(logrus.Fields{
		"module":      "recording",
		"session_id":  session.ID,
		"clip_url":    clipURL,
		"poster_url":  posterURL,
		"duration":    time.Since(session.StartTime).String(),
	}).Infoln("Successfully uploaded VSS files to Digital Ocean Spaces")

	if err := cleanupLocalFiles(session); err != nil {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"error":      err.Error(),
		}).Warnln("Failed to cleanup local files after upload")
	}

	return clipURL, posterURL, nil
}

// cleanupLocalFiles removes local files and temporary directories after successful upload
func cleanupLocalFiles(session *RecordingSession) error {
	if _, err := os.Stat(session.FilePath); err == nil {
		if err := os.Remove(session.FilePath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"file_path":  session.FilePath,
				"error":      err.Error(),
			}).Errorln("Failed to delete local video file after upload")
			return fmt.Errorf("failed to delete video file: %v", err)
		}
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"file_path":  session.FilePath,
		}).Infoln("Local video file deleted after successful upload")
	}

	if _, err := os.Stat(session.PosterPath); err == nil {
		if err := os.Remove(session.PosterPath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"poster_path": session.PosterPath,
				"error":      err.Error(),
			}).Warnln("Failed to delete local poster file after upload")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"poster_path": session.PosterPath,
			}).Infoln("Local poster file deleted after successful upload")
		}
	}

	if err := cleanupTemporaryDirectories(session); err != nil {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"error":      err.Error(),
		}).Warnln("Failed to cleanup temporary directories")
	}

	return nil
}

// cleanupTemporaryDirectories removes empty temporary directories
func cleanupTemporaryDirectories(session *RecordingSession) error {
	if strings.Contains(session.FilePath, "temp/recordings/") {
		sessionDir := filepath.Dir(session.FilePath)
		if err := os.Remove(sessionDir); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"session_dir": sessionDir,
			}).Debugln("Session directory cleanup (may not be empty)")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"session_dir": sessionDir,
			}).Infoln("Session temporary directory cleaned up")
		}

		tempRecordingsDir := filepath.Dir(sessionDir)
		if err := os.Remove(tempRecordingsDir); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"temp_dir":   tempRecordingsDir,
			}).Debugln("Temp recordings directory cleanup (may not be empty)")
		}
	} else if strings.Contains(session.FilePath, "vss/") {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
		}).Debugln("VSS files cleaned up, keeping directory structure")
	}

	return nil
}

// uploadVSSFile uploads a single file to Digital Ocean Spaces using AWS SDK v2
func uploadVSSFile(client *s3.Client, bucket, filePath, contentType string, session *RecordingSession) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get file info: %v", err)
	}

	objectKey := filepath.ToSlash(strings.TrimPrefix(filePath, "/"))

	metadata := map[string]string{
		"stream-id":        session.StreamID,
		"channel-id":       session.ChannelID,
		"session-id":       session.ID,
		"incident-id":      session.IncidentID,
		"incident-type":    session.IncidentType,
		"store-id":         session.StoreID,
		"start-time":       session.StartTime.Format(time.RFC3339),
		"duration":         time.Since(session.StartTime).String(),
		"recording-method": map[bool]string{true: "ffmpeg", false: "native"}[session.UseFFmpeg],
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(objectKey),
		Body:          file,
		ContentLength: aws.Int64(fileInfo.Size()),
		ContentType:   aws.String(contentType),
		Metadata:      metadata,
		ACL:           types.ObjectCannedACLPublicRead,
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload to Digital Ocean: %v", err)
	}

	doConfig := getDOConfigFromEnv()
	publicURL := fmt.Sprintf("https://%s.%s/%s", bucket, doConfig.Endpoint, objectKey)

	log.WithFields(logrus.Fields{
		"module":      "recording",
		"session_id":  session.ID,
		"object_key":  objectKey,
		"file_size":   fileInfo.Size(),
		"public_url":  publicURL,
		"content_type": contentType,
	}).Debugln("File uploaded to Digital Ocean Spaces")

	return publicURL, nil
}

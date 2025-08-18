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
	"github.com/sirupsen/logrus"
)

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

	envConfig := GetEnvConfig()
	if envConfig.DeleteLocalAfterUpload {
		if err := os.Remove(session.FilePath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Warnln("Failed to delete local video file after upload")
		}
		
		if err := os.Remove(session.PosterPath); err != nil && !os.IsNotExist(err) {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Warnln("Failed to delete local poster file after upload")
		}
		
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
		}).Infoln("Local VSS files deleted after successful upload")
	}

	return clipURL, posterURL, nil
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

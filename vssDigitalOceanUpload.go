package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
)

// getDOConfigFromEnv reads Digital Ocean configuration from .env file
func getDOConfigFromEnv() *DOConfig {
	config := GetEnvConfig()
	return &DOConfig{
		AccessKey: config.DOSpacesAccessKey,
		SecretKey: config.DOSpacesSecretKey,
		Endpoint:  config.DOSpacesEndpoint,
		Region:    config.DOSpacesRegion,
		Bucket:    config.DOSpacesBucket,
	}
}

// uploadVSSToDigitalOcean uploads VSS recording and poster to Digital Ocean Spaces
func uploadVSSToDigitalOcean(session *RecordingSession) (string, string, error) {
	config := getDOConfigFromEnv()

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
		"bucket":        config.Bucket,
		"endpoint":      config.Endpoint,
	}).Infoln("Starting VSS upload to Digital Ocean Spaces")

	awsConfig := &aws.Config{
		Credentials: credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:    aws.String(config.Endpoint),
		Region:      aws.String(config.Region),
		S3ForcePathStyle: aws.Bool(false),
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return "", "", fmt.Errorf("failed to create AWS session: %v", err)
	}

	svc := s3.New(sess)

	clipURL, err := uploadVSSFile(svc, config.Bucket, session.FilePath, "video/mp4", session)
	if err != nil {
		session.mutex.Lock()
		session.Status = "upload_failed"
		session.mutex.Unlock()
		return "", "", fmt.Errorf("failed to upload video: %v", err)
	}

	var posterURL string
	if _, err := os.Stat(session.PosterPath); err == nil {
		posterURL, err = uploadVSSFile(svc, config.Bucket, session.PosterPath, "image/jpeg", session)
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

// uploadVSSFile uploads a single file to Digital Ocean Spaces
func uploadVSSFile(svc *s3.S3, bucket, filePath, contentType string, session *RecordingSession) (string, error) {
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

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(objectKey),
		Body:          file,
		ContentLength: aws.Int64(fileInfo.Size()),
		ContentType:   aws.String(contentType),
		Metadata: map[string]*string{
			"stream-id":      aws.String(session.StreamID),
			"channel-id":     aws.String(session.ChannelID),
			"session-id":     aws.String(session.ID),
			"incident-id":    aws.String(session.IncidentID),
			"incident-type":  aws.String(session.IncidentType),
			"store-id":       aws.String(session.StoreID),
			"start-time":     aws.String(session.StartTime.Format(time.RFC3339)),
			"duration":       aws.String(time.Since(session.StartTime).String()),
			"recording-method": aws.String(map[bool]string{true: "ffmpeg", false: "native"}[session.UseFFmpeg]),
		},
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload to Digital Ocean: %v", err)
	}

	config := getDOConfigFromEnv()
	publicURL := fmt.Sprintf("https://%s.%s/%s", bucket, config.Endpoint, objectKey)

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

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
		"is_vss_video":  strings.Contains(session.FilePath, "vss/"),
		"is_vss_poster": strings.Contains(session.PosterPath, "vss/"),
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

	var clipURL, posterURL string
	var uploadErr error

	clipURL, uploadErr = uploadVSSFile(client, doConfig.Bucket, session.FilePath, "video/mp4", session)
	if uploadErr != nil {
		session.mutex.Lock()
		session.Status = "upload_failed"
		session.mutex.Unlock()
		
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"error":      uploadErr.Error(),
		}).Errorln("Failed to upload video file")

		if cleanupErr := cleanupLocalFiles(session); cleanupErr != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      cleanupErr.Error(),
			}).Warnln("Failed to cleanup local files after upload failure")
		}
		
		return "", "", fmt.Errorf("failed to upload video: %v", uploadErr)
	}

	if _, err := os.Stat(session.PosterPath); err == nil {
		posterURL, uploadErr = uploadVSSFile(client, doConfig.Bucket, session.PosterPath, "image/jpeg", session)
		if uploadErr != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      uploadErr.Error(),
			}).Warnln("Failed to upload poster, continuing with video only")
		}
	} else {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"poster_path": session.PosterPath,
		}).Infoln("Poster file does not exist, skipping poster upload")
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

	var objectKey string
	
	if strings.Contains(filePath, "vss/") {
		objectKey = filepath.ToSlash(strings.TrimPrefix(filePath, "/"))
	} else {
		currentTime := time.Now()
		shortUUID := generateShortUUID()
		convertedIncidentType := convertIncidentType(session.IncidentType)
		
		if strings.Contains(filePath, ".mp4") {
			videoFileName := fmt.Sprintf("%s%s.mp4", 
				currentTime.Format("20060102_150405"), 
				shortUUID)
			objectKey = fmt.Sprintf("vss/alerts/%s/%s/%s", convertedIncidentType, session.StoreID, videoFileName)
		} else {
			posterFileName := fmt.Sprintf("%s_%s.jpg", 
				currentTime.Format("20060102150405"), 
				shortUUID)
			objectKey = fmt.Sprintf("vss/posters/%s/%s/%s", convertedIncidentType, session.StoreID, posterFileName)
		}
	}

	log.WithFields(logrus.Fields{
		"module":       "recording",
		"session_id":   session.ID,
		"file_path":    filePath,
		"object_key":   objectKey,
		"file_size":    fileInfo.Size(),
		"content_type": contentType,
	}).Infoln("Uploading file to Digital Ocean Spaces")

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

	fileContent, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("failed to read file content: %v", err)
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(fileContent),
		ContentLength: aws.Int64(int64(len(fileContent))),
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
		"file_size":   len(fileContent),
		"public_url":  publicURL,
		"content_type": contentType,
		"acl":         "public-read",
	}).Infoln("File uploaded to Digital Ocean Spaces with public access")

	return publicURL, nil
}

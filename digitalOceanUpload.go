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

type DOConfig struct {
	AccessKey string
	SecretKey string
	Endpoint  string
	Region    string
	Bucket    string
}

// getDOConfig reads Digital Ocean configuration from .env file
func getDOConfig() *DOConfig {
	return getDOConfigFromEnv()
}

// uploadToDigitalOcean uploads a recording file to Digital Ocean Spaces
func uploadToDigitalOcean(session *RecordingSession) error {
	config := getDOConfigFromEnv()

	if !IsDigitalOceanConfigured() {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
		}).Warnln("Digital Ocean Spaces not configured in .env file, skipping upload")
		return nil
	}

	if _, err := os.Stat(session.FilePath); os.IsNotExist(err) {
		return fmt.Errorf("recording file does not exist: %s", session.FilePath)
	}

	session.mutex.Lock()
	session.Status = "uploading"
	session.mutex.Unlock()

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"session_id": session.ID,
		"file_path":  session.FilePath,
		"bucket":     config.Bucket,
		"endpoint":   config.Endpoint,
	}).Infoln("Starting upload to Digital Ocean Spaces")

	awsConfig := &aws.Config{
		Credentials: credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:    aws.String(config.Endpoint),
		Region:      aws.String(config.Region),
		S3ForcePathStyle: aws.Bool(false),
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %v", err)
	}

	svc := s3.New(sess)

	file, err := os.Open(session.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}

	fileName := filepath.Base(session.FilePath)
	objectKey := fmt.Sprintf("recordings/%s/%s/%s", 
		session.StreamID, 
		time.Now().Format("2006/01/02"), 
		fileName)

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(config.Bucket),
		Key:           aws.String(objectKey),
		Body:          file,
		ContentLength: aws.Int64(fileInfo.Size()),
		ContentType:   aws.String("video/mp4"),
		Metadata: map[string]*string{
			"stream-id":    aws.String(session.StreamID),
			"channel-id":   aws.String(session.ChannelID),
			"session-id":   aws.String(session.ID),
			"start-time":   aws.String(session.StartTime.Format(time.RFC3339)),
			"duration":     aws.String(time.Since(session.StartTime).String()),
			"recording-method": aws.String(map[bool]string{true: "ffmpeg", false: "native"}[session.UseFFmpeg]),
		},
	})

	if err != nil {
		session.mutex.Lock()
		session.Status = "upload_failed"
		session.mutex.Unlock()
		return fmt.Errorf("failed to upload to Digital Ocean: %v", err)
	}

	if err := generateAndUploadPoster(session, svc, config.Bucket, objectKey); err != nil {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"error":      err.Error(),
		}).Warnln("Failed to generate/upload poster")
	}

	session.mutex.Lock()
	session.Status = "uploaded"
	session.mutex.Unlock()

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"session_id": session.ID,
		"object_key": objectKey,
		"file_size":  fileInfo.Size(),
		"duration":   time.Since(session.StartTime).String(),
	}).Infoln("Successfully uploaded to Digital Ocean Spaces")

	envConfig := GetEnvConfig()
	if envConfig.DeleteLocalAfterUpload {
		if err := os.Remove(session.FilePath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Warnln("Failed to delete local file after upload")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
			}).Infoln("Local file deleted after successful upload")
		}
	}

	return nil
}

// generateAndUploadPoster generates a poster image and uploads it to DO Spaces
func generateAndUploadPoster(session *RecordingSession, svc *s3.S3, bucket, videoObjectKey string) error {
	posterPath := strings.TrimSuffix(session.FilePath, filepath.Ext(session.FilePath)) + "_poster.jpg"

	cmd := exec.Command("ffmpeg",
		"-i", session.FilePath,
		"-ss", "00:00:05", // Extract frame at 5 seconds
		"-vframes", "1",
		"-q:v", "2", // High quality
		"-y", // Overwrite output file
		"-an", // Disable audio
		posterPath,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate poster: %v", err)
	}

	defer os.Remove(posterPath)

	posterFile, err := os.Open(posterPath)
	if err != nil {
		return fmt.Errorf("failed to open poster file: %v", err)
	}
	defer posterFile.Close()

	posterInfo, err := posterFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get poster file info: %v", err)
	}

	posterObjectKey := strings.TrimSuffix(videoObjectKey, ".mp4") + "_poster.jpg"

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(posterObjectKey),
		Body:          posterFile,
		ContentLength: aws.Int64(posterInfo.Size()),
		ContentType:   aws.String("image/jpeg"),
		Metadata: map[string]*string{
			"stream-id":    aws.String(session.StreamID),
			"channel-id":   aws.String(session.ChannelID),
			"session-id":   aws.String(session.ID),
			"video-object": aws.String(videoObjectKey),
			"type":         aws.String("poster"),
		},
	})

	if err != nil {
		return fmt.Errorf("failed to upload poster to Digital Ocean: %v", err)
	}

	log.WithFields(logrus.Fields{
		"module":      "recording",
		"session_id":  session.ID,
		"poster_key":  posterObjectKey,
		"poster_size": posterInfo.Size(),
	}).Infoln("Poster uploaded to Digital Ocean Spaces")

	return nil
}

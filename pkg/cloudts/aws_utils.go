// pkg/cloudts/aws_utils.go
package cloudts

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

type S3Config struct {
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Endpoint     string // 可选，用于兼容其他S3兼容存储
}

func newAWSSession(config S3Config) *session.Session {
	awsConfig := &aws.Config{
		Region:           aws.String(config.Region),
		Endpoint:         aws.String(config.Endpoint),
		S3ForcePathStyle: aws.Bool(config.Endpoint != ""), // 如果使用自定义Endpoint，强制路径样式
	}

	// 如果有明确的访问凭证，使用它们
	if config.AccessKey != "" && config.SecretKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(
			config.AccessKey,
			config.SecretKey,
			config.SessionToken,
		)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to create AWS session: %v", err))
	}
	return sess
}

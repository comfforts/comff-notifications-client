package client

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	config "github.com/comfforts/comff-config"
	api "github.com/comfforts/comff-notifications/api/v1"
	"github.com/comfforts/logger"
)

const DEFAULT_SERVICE_PORT = "58051"
const DEFAULT_SERVICE_HOST = "127.0.0.1"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	defaultDialTimeout      = 5 * time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

const NotificationsClientContextKey = ContextKey("notifications-client")
const DefaultClientName = "comfforts-notifications-client"

type ClientOption struct {
	DialTimeout      time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
	Caller           string
}

func NewDefaultClientOption() *ClientOption {
	return &ClientOption{
		DialTimeout:      defaultDialTimeout,
		KeepAlive:        defaultKeepAlive,
		KeepAliveTimeout: defaultKeepAliveTimeout,
	}
}

type Client interface {
	GetNotificationTypes(ctx context.Context, req *api.NotificationTypesRequest, opts ...grpc.CallOption) (*api.NotificationTypesResponse, error)
	CreateNotification(ctx context.Context, req *api.CreateNotificationRequest, opts ...grpc.CallOption) (*api.NotificationResponse, error)
	GetNotification(ctx context.Context, req *api.GetNotificationRequest, opts ...grpc.CallOption) (*api.NotificationResponse, error)
	GetNotifications(ctx context.Context, req *api.GetNotificationsRequest, opts ...grpc.CallOption) (*api.NotificationsResponse, error)
	DeleteNotification(ctx context.Context, req *api.DeleteNotificationRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	Close() error
}

type notificationsClient struct {
	logger logger.AppLogger
	client api.NotificationsClient
	conn   *grpc.ClientConn
	opts   *ClientOption
}

func NewClient(
	logger logger.AppLogger,
	clientOpts *ClientOption,
) (*notificationsClient, error) {
	if clientOpts.Caller == "" {
		clientOpts.Caller = DefaultClientName
	}

	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.NOTIFICATIONS_CLIENT,
	})
	if err != nil {
		logger.Error("error setting offers service client TLS", zap.Error(err))
		return nil, err
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	servicePort := os.Getenv("NOTIFICATIONS_SERVICE_PORT")
	if servicePort == "" {
		servicePort = DEFAULT_SERVICE_PORT
	}
	serviceHost := os.Getenv("NOTIFICATIONS_SERVICE_HOST")
	if serviceHost == "" {
		serviceHost = DEFAULT_SERVICE_HOST
	}
	serviceAddr := fmt.Sprintf("%s:%s", serviceHost, servicePort)
	// with load balancer
	// serviceAddr = fmt.Sprintf("%s:///%s", loadbalance.ShopResolverName, serviceAddr)
	// serviceAddr = fmt.Sprintf("%s:///%s", "shops", serviceAddr)

	conn, err := grpc.Dial(serviceAddr, opts...)
	if err != nil {
		logger.Error("notifications client failed to connect", zap.Error(err))
		return nil, err
	}

	client := api.NewNotificationsClient(conn)
	logger.Info("notifications client connected", zap.String("host", serviceHost), zap.String("port", servicePort))
	return &notificationsClient{
		client: client,
		logger: logger,
		conn:   conn,
		opts:   clientOpts,
	}, nil
}

func (nc *notificationsClient) GetNotificationTypes(
	ctx context.Context,
	req *api.NotificationTypesRequest,
	opts ...grpc.CallOption,
) (*api.NotificationTypesResponse, error) {
	ctx, cancel := nc.contextWithOptions(ctx, nc.opts)
	defer cancel()

	return nc.client.GetNotificationTypes(ctx, req)
}

func (nc *notificationsClient) CreateNotification(
	ctx context.Context,
	req *api.CreateNotificationRequest,
	opts ...grpc.CallOption,
) (*api.NotificationResponse, error) {
	ctx, cancel := nc.contextWithOptions(ctx, nc.opts)
	defer cancel()

	return nc.client.CreateNotification(ctx, req)
}

func (nc *notificationsClient) GetNotification(
	ctx context.Context,
	req *api.GetNotificationRequest,
	opts ...grpc.CallOption,
) (*api.NotificationResponse, error) {
	ctx, cancel := nc.contextWithOptions(ctx, nc.opts)
	defer cancel()

	return nc.client.GetNotification(ctx, req)
}

func (nc *notificationsClient) GetNotifications(
	ctx context.Context,
	req *api.GetNotificationsRequest,
	opts ...grpc.CallOption,
) (*api.NotificationsResponse, error) {
	ctx, cancel := nc.contextWithOptions(ctx, nc.opts)
	defer cancel()

	return nc.client.GetNotifications(ctx, req)
}

func (nc *notificationsClient) DeleteNotification(
	ctx context.Context,
	req *api.DeleteNotificationRequest,
	opts ...grpc.CallOption,
) (*api.DeleteResponse, error) {
	ctx, cancel := nc.contextWithOptions(ctx, nc.opts)
	defer cancel()

	return nc.client.DeleteNotification(ctx, req)
}

func (nc *notificationsClient) Close() error {
	if err := nc.conn.Close(); err != nil {
		nc.logger.Error("error closing notifications client connection", zap.Error(err))
		return err
	}
	return nil
}

func (nc *notificationsClient) contextWithOptions(ctx context.Context, opts *ClientOption) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, nc.opts.DialTimeout)
	if nc.opts.Caller != "" {
		md := metadata.New(map[string]string{"service-client": nc.opts.Caller})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return ctx, cancel
}

package client_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	api "github.com/comfforts/comff-notifications/api/v1"
	"github.com/comfforts/logger"

	notclient "github.com/comfforts/comff-notifications-client"
)

const TEST_DIR = "data"
const TEST_SHOP_ID = "test-notification-client-shop"
const TEST_COURIER_ID = "test-notification-client-courier"

func TestOffersClient(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		nc notclient.Client,
	){
		"test database setup check, succeeds": testDatabaseSetup,
		"test notification CRUD, succeeds":    testNotificationCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			nc, teardown := setup(t, logger)
			defer teardown()
			fn(t, nc)
		})
	}

}

func setup(t *testing.T, logger logger.AppLogger) (
	nc notclient.Client,
	teardown func(),
) {
	t.Helper()

	clientOpts := notclient.NewDefaultClientOption()
	clientOpts.Caller = "notifications-client-test"

	nc, err := notclient.NewClient(logger, clientOpts)
	require.NoError(t, err)

	return nc, func() {
		t.Logf(" %s ended, will clean up", t.Name())
		err := nc.Close()
		require.NoError(t, err)
	}
}

func testDatabaseSetup(t *testing.T, nc notclient.Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsResp, err := nc.GetNotificationTypes(ctx, &api.NotificationTypesRequest{})
	require.NoError(t, err)
	require.Equal(t, len(dsResp.Types), 1)
}

func testNotificationCRUD(t *testing.T, nc notclient.Client) {
	t.Helper()

	deliveryId, offerId := "test-server-notification-delivery", "test-server-shop-delivery-offer"
	noti := createNotificationTester(t, nc, &api.CreateNotificationRequest{
		ActorId:       TEST_SHOP_ID,
		SubjectId:     deliveryId,
		TransactionId: offerId,
		Content:       "from shop",
		Type:          api.NotificationType_DELIVERY,
	})
	noti = getNotificationTester(t, nc, &api.GetNotificationRequest{
		Id: noti.Notification.Id,
	})

	deleteNotificationTester(t, nc, &api.DeleteNotificationRequest{
		Id: noti.Notification.Id,
	})
}

func createNotificationTester(t *testing.T, client notclient.Client, cor *api.CreateNotificationRequest) *api.NotificationResponse {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.CreateNotification(ctx, cor)
	require.NoError(t, err)
	assert.Equal(t, resp.Notification.Record.ActorId, cor.ActorId, "notification actor id should match input actor id")
	assert.Equal(t, resp.Notification.Record.SubjectId, cor.SubjectId, "notification subject id should match input subject id")
	assert.Equal(t, resp.Notification.Record.TransactionId, cor.TransactionId, "notification transaction id should match input transaction id")
	assert.Equal(t, resp.Notification.Content, cor.Content, "notification Content should match input Content")
	assert.Equal(t, resp.Notification.Type, cor.Type, "notification type should match input type")

	return resp
}

func getNotificationTester(t *testing.T, client notclient.Client, gor *api.GetNotificationRequest) *api.NotificationResponse {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.GetNotification(ctx, gor)
	require.NoError(t, err)
	require.Equal(t, resp.Notification.Id, gor.Id)
	return resp
}

func deleteNotificationTester(t *testing.T, client notclient.Client, dor *api.DeleteNotificationRequest) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.DeleteNotification(ctx, dor)
	require.NoError(t, err)
	require.Equal(t, true, resp.Ok)
}

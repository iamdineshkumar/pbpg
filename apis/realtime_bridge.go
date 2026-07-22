package apis

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/pocketbase/pocketbase/tools/security"
	"github.com/pocketbase/pocketbase/tools/subscriptions"
)

type IRealtimeBridge interface {
	App() core.App
	SelfChannelId() string
	SendViaBridge(channelId string, clientId string, message subscriptions.Message)
}

var _ IRealtimeBridge = (*RealtimeBridge)(nil)

type RealtimeBridge struct {
	channelId string
	app       core.App
}

var RealtimeBridgeInstanceKey = "realtime_bridge_instance"

func bindRealtimeBridge(app core.App) {
	ctx, cancel := context.WithCancel(context.Background())
	bridge := &RealtimeBridge{
		channelId: genChannelId(),
		app:       app,
	}

	app.OnServe().Bind(&hook.Handler[*core.ServeEvent]{
		Func: func(e *core.ServeEvent) error {
			bridge.mustCreateTables()
			go bridge.heartbeatLoop(ctx)
			go bridge.listenSharedBridgeChannelLoop(ctx)
			go bridge.listenBridgeMessagesLoop(ctx)

			app.Store().Set(RealtimeBridgeInstanceKey, bridge)

			return e.Next()
		},
	})

	app.OnTerminate().Bind(&hook.Handler[*core.TerminateEvent]{
		Func: func(e *core.TerminateEvent) error {
			cancel()
			return e.Next()
		},
	})

	app.OnCollectionCreateRequest().Bind(&hook.Handler[*core.CollectionRequestEvent]{
		Func: bridge.broadcastCollectionChanged,
	})
	app.OnCollectionUpdateRequest().Bind(&hook.Handler[*core.CollectionRequestEvent]{
		Func: bridge.broadcastCollectionChanged,
	})
	app.OnCollectionDeleteRequest().Bind(&hook.Handler[*core.CollectionRequestEvent]{
		Func: bridge.broadcastCollectionChanged,
	})
	app.OnSettingsUpdateRequest().Bind(&hook.Handler[*core.SettingsUpdateRequestEvent]{
		Func: bridge.broadcastSettingsUpdated,
	})
}

func (t *RealtimeBridge) listenSharedBridgeChannelLoop(ctx context.Context) {
	loopOnNotification(ctx, t.app, "shared_bridge_channel", func() {
		t.fullRefreshSubscriptions()
		_ = t.app.ReloadCachedCollections()
		_ = t.app.ReloadSettings()
	}, func(notification *pgconn.Notification) {
		if t.app.IsDev() {
			fmt.Println("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
		}

		messageType, messagePayload, ok := split2(notification.Payload, "|")
		if !ok {
			fmt.Fprintln(os.Stderr, "Invalid notification payload:", notification.Payload)
			return
		}

		switch messageType {
		case "subscription_upsert":
			subscriptionJson, authRecordJson, ok := split2(messagePayload, "|")
			if !ok {
				fmt.Fprintln(os.Stderr, "Invalid subscriptionChange payload:", messagePayload)
				return
			}

			var subscription ClientSubscription
			err := json.Unmarshal([]byte(subscriptionJson), &subscription)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error unmarshalling notification payload:", err)
				os.Exit(1)
			}
			if subscription.UpdatedByChannelId == t.SelfChannelId() {
				return
			}

			var client IBridgedClient
			if c, err := t.app.SubscriptionsBroker().ClientById(subscription.ClientId); err == nil {
				client = c.(IBridgedClient)
			} else {
				client = NewBridgedClient(t, &subscription)
				t.app.SubscriptionsBroker().Register(client)
			}
			client.ReceiveChanges(&subscription, authRecordJson)
		case "subscription_delete":
			clientId := messagePayload
			t.app.SubscriptionsBroker().Unregister(clientId)
		case "subscription_channel_offline":
			channelId := messagePayload
			for _, c := range t.app.SubscriptionsBroker().Clients() {
				if syncClient, ok := c.(IBridgedClient); ok && syncClient.ClientSubscription().ChannelId == channelId {
					t.app.SubscriptionsBroker().Unregister(syncClient.Id())
				}
			}
		case "collection_updated":
			_ = t.app.ReloadCachedCollections()
		case "settings_updated":
			_ = t.app.ReloadSettings()
		default:
			fmt.Fprintln(os.Stderr, "Unknown change type:", messageType)
			panic("Unknown change type: " + messageType)
		}
	})
}

func (t *RealtimeBridge) SendViaBridge(channelId string, clientId string, message subscriptions.Message) {
	if channelId == t.SelfChannelId() {
		fmt.Fprintln(os.Stderr, "Cannot send bridged message to self channel:", channelId)
		panic("Cannot send bridged message to self channel")
	}
	_, err := t.app.DB().NewQuery(`
		SELECT pg_notify({:channelId}, {:payload})
	`).Bind(dbx.Params{
		"channelId": channelId,
		"payload":   clientId + "|" + message.Name + "|" + string(message.Data),
	}).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error sending notification:", err)
		os.Exit(1)
	}
}

func (b *RealtimeBridge) listenBridgeMessagesLoop(ctx context.Context) {
	loopOnNotification(ctx, b.app, b.SelfChannelId(), nil, func(notification *pgconn.Notification) {
		if b.app.IsDev() {
			fmt.Println("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
		}

		clientId, messageName, messageData, ok := split3(notification.Payload, "|")
		if !ok {
			fmt.Fprintln(os.Stderr, "Invalid notification payload:", notification.Payload)
			return
		}
		var client IBridgedClient
		if c, err := b.app.SubscriptionsBroker().ClientById(clientId); err == nil {
			client = c.(IBridgedClient)
		} else {
			fmt.Fprintln(os.Stderr, "Client not found, it may be already disconnected:", clientId)
			return
		}
		if client.IsRemoteClient() {
			fmt.Fprintln(os.Stderr, "Message is sent to the wrong channel:", notification.Payload)
			return
		}
		client.Send(subscriptions.Message{
			Name: messageName,
			Data: []byte(messageData),
		})
	})
}

var pgTypes = pgtype.NewMap()

func (t *RealtimeBridge) fullRefreshSubscriptions() {
	rows, err := t.app.DB().NewQuery(`
		SELECT "clientId", "channelId", "subscriptions", "authCollectionRef", "authRecordRef", "updatedByChannelId"
		FROM "_realtimeClients"
		WHERE "updatedByChannelId" != {:selfChannelId}
	`).Bind(dbx.Params{
		"selfChannelId": t.SelfChannelId(),
	}).Rows()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error loading subscriptions:", err)
	}
	defer rows.Close()

	clientsToRemove := make(map[string]any)
	for _, c := range t.app.SubscriptionsBroker().Clients() {
		if client, ok := c.(IBridgedClient); ok && client.IsRemoteClient() {
			clientsToRemove[client.Id()] = nil
		}
	}

	for rows.Next() {
		var subscription ClientSubscription
		err := rows.Scan(
			&subscription.ClientId,
			&subscription.ChannelId,
			pgTypes.SQLScanner(&subscription.Subscriptions),
			&subscription.AuthCollectionRef,
			&subscription.AuthRecordRef,
			&subscription.UpdatedByChannelId,
		)
		if err != nil {
			data := make(dbx.NullStringMap, 0)
			err := rows.ScanMap(data)
			fmt.Fprintln(os.Stderr, "Error scanning subscription:", err)
			continue
		}
		var client IBridgedClient
		if c, err := t.app.SubscriptionsBroker().ClientById(subscription.ClientId); err == nil {
			client = c.(IBridgedClient)
		} else {
			client = NewBridgedClient(t, &subscription)
			t.app.SubscriptionsBroker().Register(client)
		}
		client.ReceiveChanges(&subscription, "")
		delete(clientsToRemove, client.Id())
	}
	for clientId := range clientsToRemove {
		t.app.SubscriptionsBroker().Unregister(clientId)
	}
}

func (t *RealtimeBridge) mustCreateTables() {
	_, err := t.app.DB().NewQuery(`
		CREATE TABLE IF NOT EXISTS "_realtimeChannels" (
			"channelId" TEXT PRIMARY KEY,
			"validUntil" TIMESTAMP NOT NULL
		);
		CREATE TABLE IF NOT EXISTS "_realtimeClients" (
			"clientId" TEXT NOT NULL PRIMARY KEY,
			"channelId" TEXT NOT NULL,
			"subscriptions" TEXT[] NOT NULL,
			"authCollectionRef" TEXT NOT NULL DEFAULT '',
			"authRecordRef" TEXT NOT NULL DEFAULT '',
			"updatedByChannelId" TEXT NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS "_realtimeClients_channelId_idx" ON "_realtimeClients" ("channelId");
	`).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating tables tables:", err)
		os.Exit(1)
	}
}

func (t *RealtimeBridge) heartbeatLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "Stopping realtime sync heartbeat loop.")
			return
		default:
			if t.app.DB() == nil {
				fmt.Fprintln(os.Stderr, "App is not initialized or stopped, stopping realtime sync heartbeat loop.")
				break
			}
			_, err := t.app.DB().NewQuery(`
				WITH 
					insert_operation AS (
						INSERT INTO "_realtimeChannels" ("channelId", "validUntil")
						VALUES ({:channelId}, now() + interval '40 seconds')
						ON CONFLICT ("channelId") DO UPDATE
						SET "validUntil" = EXCLUDED."validUntil"
					),
					deleted_channels AS (
						DELETE FROM "_realtimeChannels"
						WHERE "validUntil" < now()
						RETURNING "channelId"
					),
					_ AS (
						DELETE FROM "_realtimeClients"
						WHERE "channelId" IN (SELECT "channelId" FROM deleted_channels)
					)
				SELECT pg_notify('shared_bridge_channel', 'subscription_channel_offline|' || "channelId") FROM deleted_channels;
			`).Bind(dbx.Params{
				"channelId": t.channelId,
			}).Execute()
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error sending heartbeat:", err)
				time.Sleep(time.Second * 1)
				continue
			}
			jitter := time.Second * time.Duration(rand.Intn(5))
			time.Sleep(time.Second*30 - jitter)
		}
	}
}

func (t *RealtimeBridge) SelfChannelId() string {
	return t.channelId
}

func (t *RealtimeBridge) App() core.App {
	return t.app
}

func genChannelId() string {
	hostname, _ := os.Hostname()
	randstr, _ := security.RandomStringByRegex(`[a-z0-9]{5}`)
	channelId := "c" + "_" + hostname + "_" + randstr

	channelId = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '_'
	}, channelId)

	return channelId
}

func loopOnNotification(ctx context.Context, app core.App, channel string, afterListenFunc func(), handler func(*pgconn.Notification)) {
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "Stopping PostgreSQL stream listener loop on channel:", channel)
			return
		default:
			err := runPgxCommand(ctx, app, func(pgxConn *pgx.Conn) error {
				_, err := pgxConn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))
				if err != nil {
					return fmt.Errorf("error listening on channel %s: %w", channel, err)
				}

				if afterListenFunc != nil {
					afterListenFunc()
				}

				for {
					notification, err := pgxConn.WaitForNotification(ctx)
					if err != nil {
						if err == context.Canceled {
							app.Logger().Info("Context was canceled, exiting the loop on channel", "channel", channel)
							return nil
						}
						return fmt.Errorf("error waiting for notification on channel %s: %w", channel, err)
					}
					handler(notification)
				}
			})
			if err != nil {
				app.Logger().Error("Error in PostgreSQL stream listener loop on channel", "channel", channel, "error", err)
				fmt.Fprintln(os.Stderr, "Error in PostgreSQL stream listener loop on channel", channel, ":", err)
				time.Sleep(time.Second * 1)
				continue
			}
		}
	}
}

func (t *RealtimeBridge) broadcastCollectionChanged(e *core.CollectionRequestEvent) error {
	if err := e.Next(); err != nil {
		return err
	}
	sql := `SELECT pg_notify('shared_bridge_channel', 'collection_updated|' || 'empty-payload')`
	if _, err := t.app.DB().NewQuery(sql).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error sending [collection_updated] notification:", err)
	}
	return nil
}

func (t *RealtimeBridge) broadcastSettingsUpdated(e *core.SettingsUpdateRequestEvent) error {
	if err := e.Next(); err != nil {
		return err
	}
	sql := `SELECT pg_notify('shared_bridge_channel', 'settings_updated|' || 'empty-payload')`
	if _, err := t.app.DB().NewQuery(sql).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error sending [settings_updated] notification:", err)
	}
	return nil
}

func runPgxCommand(ctx context.Context, app core.App, pgxFunc func(*pgx.Conn) error) error {
	dbxDB, _ := app.NonconcurrentDB().(*dbx.DB)
	if dbxDB == nil {
		return fmt.Errorf("app.NonconcurrentDB() is not a *dbx.DB instance")
	}
	sqlDB := dbxDB.DB()
	sqlConn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring SQL connection: %w", err)
	}
	defer sqlConn.Close()

	return sqlConn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("driverConn is not a *stdlib.Conn instance, are you using the pgx's database/sql driver?")
		}
		pgxConn := stdlibConn.Conn()
		return pgxFunc(pgxConn)
	})
}

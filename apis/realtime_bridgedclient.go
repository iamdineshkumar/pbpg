package apis

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/postgresbase/dbx"
	"github.com/postgresbase/pocketbase/core"
	"github.com/postgresbase/pocketbase/tools/subscriptions"
)

type IBridgedClient interface {
	subscriptions.Client

	ClientSubscription() *ClientSubscription

	IsRemoteClient() bool

	BroadcastGoOffline()

	BroadcastChanges()

	ReceiveChanges(newSubscription *ClientSubscription, authRecordJson string)
}

type ClientSubscription struct {
	ClientId           string   `db:"pk,clientId" json:"clientId"`
	ChannelId          string   `db:"channelId" json:"channelId"`
	Subscriptions      []string `db:"subscriptions" json:"subscriptions"`
	AuthCollectionRef  string   `db:"authCollectionRef" json:"authCollectionRef"`
	AuthRecordRef      string   `db:"authRecordRef" json:"authRecordRef"`
	UpdatedByChannelId string   `db:"updatedByChannelId" json:"updatedByChannelId"`
}

var _ subscriptions.Client = (*BridgedClient)(nil)
var _ IBridgedClient = (*BridgedClient)(nil)

type BridgedClient struct {
	subscriptions.Client
	bridge       IRealtimeBridge
	subscription *ClientSubscription
}

func NewBridgedClient(bridge IRealtimeBridge, optionalSubscription ...*ClientSubscription) *BridgedClient {
	var client subscriptions.Client
	var subscription *ClientSubscription

	if len(optionalSubscription) > 0 && optionalSubscription[0] != nil {
		client = subscriptions.NewDefaultClient(optionalSubscription[0].ClientId)
		subscription = optionalSubscription[0]
	} else {
		client = subscriptions.NewDefaultClient()
		subscription = &ClientSubscription{
			ClientId:           client.Id(),
			ChannelId:          bridge.SelfChannelId(),
			Subscriptions:      []string{},
			AuthCollectionRef:  "",
			AuthRecordRef:      "",
			UpdatedByChannelId: "",
		}
	}

	return &BridgedClient{
		bridge:       bridge,
		Client:       client,
		subscription: subscription,
	}
}

func (r *BridgedClient) BroadcastGoOffline() {
	_, err := r.bridge.App().DB().NewQuery(`
		WITH deleted AS (
			DELETE FROM "_realtimeClients"
			WHERE "clientId" = {:clientId}
			RETURNING *
		)
		SELECT pg_notify('shared_bridge_channel', 'subscription_delete|' || deleted."clientId") FROM deleted;
	`).Bind(dbx.Params{
		"clientId": r.subscription.ClientId,
	}).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error deleting subscription:", err)
		return
	}
}

func (r *BridgedClient) BroadcastChanges() {
	if len(r.RawSubscriptions()) == 0 {
		r.BroadcastGoOffline()
		return
	}

	r.subscription.Subscriptions = r.RawSubscriptions()
	r.subscription.UpdatedByChannelId = r.bridge.SelfChannelId()

	var authSQL string
	var authParams dbx.Params
	if record, _ := r.Get(RealtimeClientAuthKey).(*core.Record); record != nil {
		r.subscription.AuthCollectionRef = record.Collection().Id
		r.subscription.AuthRecordRef = record.Id
		authSQL = fmt.Sprintf(`SELECT * FROM {{%s}} WHERE id = {:authRecordRef} LIMIT 1`, record.TableName())
		authParams = dbx.Params{
			"authRecordRef": record.Id,
		}
	} else {
		r.subscription.AuthCollectionRef = ""
		r.subscription.AuthRecordRef = ""
		authSQL = "SELECT WHERE 1=0"
	}

	_, err := r.bridge.App().DB().NewQuery(fmt.Sprintf(`
		WITH
			updated AS (
				INSERT INTO "_realtimeClients" (
					"clientId", "channelId", "subscriptions", "authCollectionRef", "authRecordRef", "updatedByChannelId"
				) VALUES (
					{:clientId}, {:channelId}, {:subscriptions}, {:authCollectionRef}, {:authRecordRef}, {:updatedByChannelId}
				) ON CONFLICT ("clientId") DO UPDATE
				SET "subscriptions" = EXCLUDED."subscriptions",
					"authCollectionRef" = EXCLUDED."authCollectionRef",
					"authRecordRef" = EXCLUDED."authRecordRef",
					"updatedByChannelId" = EXCLUDED."updatedByChannelId"
				RETURNING *
			),
			auth AS (
				%s
			)
		SELECT pg_notify(
			'shared_bridge_channel', 
			CONCAT_WS(
				'|',
				'subscription_upsert', 
				(SELECT row_to_json(u) FROM updated u)::text, 
				COALESCE((SELECT row_to_json(a) FROM auth a)::text, '')
			)
		);
	`, authSQL)).Bind(authParams).Bind(dbx.Params{
		"clientId":           r.subscription.ClientId,
		"channelId":          r.subscription.ChannelId,
		"subscriptions":      r.subscription.Subscriptions,
		"authCollectionRef":  r.subscription.AuthCollectionRef,
		"authRecordRef":      r.subscription.AuthRecordRef,
		"updatedByChannelId": r.subscription.UpdatedByChannelId,
	}).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error updating subscription:", err)
		return
	}
}

func (r *BridgedClient) ReceiveChanges(newSubscription *ClientSubscription, authRecordJson string) {
	r.Client.Unsubscribe()
	r.Client.Subscribe(newSubscription.Subscriptions...)
	if newSubscription.AuthRecordRef == "" || newSubscription.AuthCollectionRef == "" {
		r.Client.Unset(RealtimeClientAuthKey)
	} else {
		authRecord, err := AuthRecordFromJson(r.bridge.App(), newSubscription.AuthCollectionRef, authRecordJson)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error unmarshalling auth record JSON:", err)
			return
		}
		r.Client.Set(RealtimeClientAuthKey, authRecord)
	}
	r.subscription = newSubscription
}

func (r *BridgedClient) Send(m subscriptions.Message) {
	if r.IsDiscarded() {
		return
	}

	if r.IsRemoteClient() {
		r.bridge.SendViaBridge(r.subscription.ChannelId, r.Id(), m)
	} else {
		r.Client.Send(m)
	}
}

func (r *BridgedClient) IsRemoteClient() bool {
	return r.subscription.ChannelId != r.bridge.SelfChannelId()
}

func (r *BridgedClient) ClientSubscription() *ClientSubscription {
	return r.subscription
}

func AuthRecordFromJson(app core.App, authCollectionNameOrId string, authRecordJson string) (*core.Record, error) {
	if authRecordJson == "" {
		return nil, nil
	}
	collection, err := app.FindCachedCollectionByNameOrId(authCollectionNameOrId)
	if err != nil {
		return nil, fmt.Errorf("error finding collection by name or ID: %w", err)
	}
	authRecord := core.NewRecord(collection)

	data := make(map[string]any)
	_ = json.Unmarshal([]byte(authRecordJson), &data)
	for _, field := range collection.Fields {
		fieldName := field.GetName()
		value, _ := field.PrepareValue(authRecord, data[fieldName])
		authRecord.SetRaw(fieldName, value)
	}
	return authRecord, nil
}

func split2(s string, sep string) (string, string, bool) {
	parts := strings.SplitN(s, sep, 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func split3(s string, sep string) (string, string, string, bool) {
	parts := strings.SplitN(s, sep, 3)
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

package whatsapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// keepSlotStubStorage is a minimal IChatStorageRepository that records the device
// registry calls exercised by the logout/purge paths and can be told to fail, so the
// tests can assert error propagation. All other interface methods are nil (embedded)
// and must not be called by the code under test.
type keepSlotStubStorage struct {
	domainChatStorage.IChatStorageRepository
	saveErr        error
	deleteDataErr  error
	listRecords    []*domainChatStorage.DeviceRecord
	savedRecords   []*domainChatStorage.DeviceRecord
	deletedData    []string
	deletedRecords []string
}

func (s *keepSlotStubStorage) SaveDeviceRecord(rec *domainChatStorage.DeviceRecord) error {
	cloned := *rec
	s.savedRecords = append(s.savedRecords, &cloned)
	return s.saveErr
}

func (s *keepSlotStubStorage) DeleteDeviceData(deviceID string) error {
	s.deletedData = append(s.deletedData, deviceID)
	return s.deleteDataErr
}

func (s *keepSlotStubStorage) DeleteDeviceRecord(deviceID string) error {
	s.deletedRecords = append(s.deletedRecords, deviceID)
	return nil
}

// ListDeviceRecords backs the legacy (no-AD-JID) ambiguity guard in deleteStoreRowsForJID.
// It returns the configured records (nil by default), letting a test model how many slots
// claim a number: nil keeps slotsClaiming at 0 (unambiguous) and preserves the original
// by-number delete path, while seeding records simulates one or more slots claiming it.
func (s *keepSlotStubStorage) ListDeviceRecords() ([]*domainChatStorage.DeviceRecord, error) {
	return s.listRecords, nil
}

// assertStoreLacksJID fails if any device row in the container still matches the given
// NonAD JID. Matching mirrors deleteStoreRowsForJID / LoadExistingDevices.
func assertStoreLacksJID(t *testing.T, ctx context.Context, c *sqlstore.Container, nonADJID string) {
	t.Helper()
	devices, err := c.GetAllDevices(ctx)
	if err != nil {
		t.Fatalf("get all devices: %v", err)
	}
	for _, d := range devices {
		if d != nil && d.ID != nil && d.ID.ToNonAD().String() == nonADJID {
			t.Fatalf("expected store to no longer contain jid %s", nonADJID)
		}
	}
}

// assertStoreLacksADJID fails if any device row in the container still matches the given
// full AD JID string. Matching is device-precise (dev.ID.String()), so a sibling companion
// of the same number with a different device suffix is intentionally not matched.
func assertStoreLacksADJID(t *testing.T, ctx context.Context, c *sqlstore.Container, adJID string) {
	t.Helper()
	devices, err := c.GetAllDevices(ctx)
	if err != nil {
		t.Fatalf("get all devices: %v", err)
	}
	for _, d := range devices {
		if d != nil && d.ID != nil && d.ID.String() == adJID {
			t.Fatalf("expected store to no longer contain ad jid %s", adJID)
		}
	}
}

// assertStoreHasADJID fails unless a device row with the given full AD JID string is present.
func assertStoreHasADJID(t *testing.T, ctx context.Context, c *sqlstore.Container, adJID string) {
	t.Helper()
	devices, err := c.GetAllDevices(ctx)
	if err != nil {
		t.Fatalf("get all devices: %v", err)
	}
	assertStoreHasDevice(t, devices, adJID)
}

// Scenario: keep-slot logout for a slot that was loaded from storage with NO live
// client (the case aldinokemal flagged at device_manager.go:240). The orphan whatsmeow
// rows must be deleted by JID from BOTH the primary and the separate keys container,
// while the slot itself (id + display name) is preserved with an empty persisted JID.
// The slot id is deliberately different from the JID to prove matching is by JID, not
// by the slot id (the latent AD-JID-vs-slot-id mismatch in the old code).
func TestLogoutDeviceKeepSlot_NoClientDeletesStoreRowsByJIDKeepsSlot(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	keysStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281999999991", types.WhatsAppDomain, 10)
	nonAD := adJID.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID, "primary").Save(ctx); err != nil {
		t.Fatalf("save primary device: %v", err)
	}
	if err := newTestStoreDevice(keysStore, adJID, "keys").Save(ctx); err != nil {
		t.Fatalf("save keys device: %v", err)
	}

	storage := &keepSlotStubStorage{}
	manager := NewDeviceManager(primaryStore, keysStore, storage)

	const slotID = "slot-uuid-1" // intentionally != JID
	inst := &DeviceInstance{id: slotID, jid: nonAD, displayName: "tIAtendo", createdAt: time.Now()}
	manager.devices[slotID] = inst

	if err := manager.LogoutDeviceKeepSlot(ctx, slotID); err != nil {
		t.Fatalf("LogoutDeviceKeepSlot returned error: %v", err)
	}

	assertStoreLacksJID(t, ctx, primaryStore, nonAD)
	assertStoreLacksJID(t, ctx, keysStore, nonAD)

	if _, ok := manager.GetDevice(slotID); !ok {
		t.Fatal("expected slot to be kept after logout")
	}
	if got := inst.JID(); got != "" {
		t.Fatalf("expected instance JID cleared after logout, got %q", got)
	}
	if len(storage.savedRecords) == 0 {
		t.Fatal("expected SaveDeviceRecord to persist the kept slot")
	}
	last := storage.savedRecords[len(storage.savedRecords)-1]
	if last.DeviceID != slotID || last.JID != "" {
		t.Fatalf("expected persisted slot %s with empty JID, got %s / %q", slotID, last.DeviceID, last.JID)
	}
}

// Scenario: DELETE purge removes the whatsmeow rows by JID from both containers even
// when the slot id differs from the JID (the old code compared the AD JID string to the
// slot id and never matched, so the rows were never deleted), and removes the slot.
func TestPurgeDevice_DeletesStoreRowsByJIDAcrossPrimaryAndKeys(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	keysStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281999999992", types.WhatsAppDomain, 11)
	nonAD := adJID.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID, "primary").Save(ctx); err != nil {
		t.Fatalf("save primary device: %v", err)
	}
	if err := newTestStoreDevice(keysStore, adJID, "keys").Save(ctx); err != nil {
		t.Fatalf("save keys device: %v", err)
	}

	storage := &keepSlotStubStorage{}
	manager := NewDeviceManager(primaryStore, keysStore, storage)

	const slotID = "slot-uuid-2"
	manager.devices[slotID] = &DeviceInstance{id: slotID, jid: nonAD, displayName: "tIAtendo", createdAt: time.Now()}

	if err := manager.PurgeDevice(ctx, slotID); err != nil {
		t.Fatalf("PurgeDevice returned error: %v", err)
	}

	assertStoreLacksJID(t, ctx, primaryStore, nonAD)
	assertStoreLacksJID(t, ctx, keysStore, nonAD)

	if _, ok := manager.GetDevice(slotID); ok {
		t.Fatal("expected slot to be removed after purge")
	}
	if len(storage.deletedData) == 0 || storage.deletedData[0] != slotID {
		t.Fatalf("expected chatstorage data deletion for %s, got %v", slotID, storage.deletedData)
	}
	if len(storage.deletedRecords) == 0 || storage.deletedRecords[0] != slotID {
		t.Fatalf("expected device record deletion for %s, got %v", slotID, storage.deletedRecords)
	}
}

// resetDeviceKeepSlot must surface a persistence failure instead of silently
// succeeding (coderabbitai device_manager.go:252-276), and LogoutDeviceKeepSlot must
// combine that error into its return value.
func TestResetDeviceKeepSlot_PropagatesSaveError(t *testing.T) {
	ctx := context.Background()
	storage := &keepSlotStubStorage{saveErr: errors.New("disk full")}
	manager := NewDeviceManager(nil, nil, storage)

	const slotID = "slot-persist-err"
	manager.devices[slotID] = &DeviceInstance{id: slotID, jid: "6281999999993@s.whatsapp.net", createdAt: time.Now()}

	if err := manager.resetDeviceKeepSlot(slotID); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected resetDeviceKeepSlot to propagate save error, got %v", err)
	}

	if err := manager.LogoutDeviceKeepSlot(ctx, slotID); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected LogoutDeviceKeepSlot to surface the persistence error, got %v", err)
	}
}

// DELETE promises a real purge: a LOCAL cleanup failure (chatstorage/store/keys) must
// be surfaced, not masked as success (aldinokemal/coderabbitai usecase device.go:68).
func TestPurgeDevice_SurfacesLocalCleanupFailure(t *testing.T) {
	ctx := context.Background()
	storage := &keepSlotStubStorage{deleteDataErr: errors.New("chatstorage down")}
	manager := NewDeviceManager(nil, nil, storage)

	const slotID = "slot-cleanup-err"
	manager.devices[slotID] = &DeviceInstance{id: slotID, createdAt: time.Now()}

	if err := manager.PurgeDevice(ctx, slotID); err == nil || !strings.Contains(err.Error(), "chatstorage down") {
		t.Fatalf("expected PurgeDevice to surface the local cleanup failure, got %v", err)
	}
}

// deleteStoreRowsForJID is a no-op for an empty JID: a slot that was never paired has no
// store rows, and an empty JID must never scan/delete anything.
func TestDeleteStoreRowsForJID_EmptyJIDIsNoOp(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	adJID := types.NewADJID("6281999999994", types.WhatsAppDomain, 12)
	if err := newTestStoreDevice(primaryStore, adJID, "primary").Save(ctx); err != nil {
		t.Fatalf("save primary device: %v", err)
	}

	manager := NewDeviceManager(primaryStore, nil, nil)
	if err := manager.deleteStoreRowsForJID(ctx, "", ""); err != nil {
		t.Fatalf("expected empty-jid no-op, got error: %v", err)
	}

	devices, err := primaryStore.GetAllDevices(ctx)
	if err != nil {
		t.Fatalf("get all devices: %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected the unrelated device to be retained, got %d devices", len(devices))
	}
}

// Scenario: the remote-logout callback holds a stale device id. InitWaCLI registers the
// callback with the instance's AD JID string (device.ID.String()), but loadFromRegistry
// can replace that instance with a named registry slot keyed by uuid (same account,
// NonAD JID). keepSlotLogout must then fall back to resolving the slot by JID, so the
// cleanup still deletes the store rows and clears the slot instead of no-opping with
// "device not found" (leaving a stale JID + orphan keys row).
func TestKeepSlotLogout_StaleADJIDFallsBackToJIDResolution(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	keysStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281999999996", types.WhatsAppDomain, 14)
	nonAD := adJID.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID, "primary").Save(ctx); err != nil {
		t.Fatalf("save primary device: %v", err)
	}
	if err := newTestStoreDevice(keysStore, adJID, "keys").Save(ctx); err != nil {
		t.Fatalf("save keys device: %v", err)
	}

	storage := &keepSlotStubStorage{}
	manager := NewDeviceManager(primaryStore, keysStore, storage)

	// The registry slot that replaced the startup instance: keyed by uuid, NonAD JID.
	const slotID = "slot-uuid-3"
	inst := &DeviceInstance{id: slotID, jid: nonAD, displayName: "tIAtendo", createdAt: time.Now()}
	manager.devices[slotID] = inst

	// The stale id the startup path registered the callback with: the AD JID string.
	if err := manager.keepSlotLogout(ctx, adJID.String()); err != nil {
		t.Fatalf("keepSlotLogout with stale AD JID id returned error: %v", err)
	}

	assertStoreLacksJID(t, ctx, primaryStore, nonAD)
	assertStoreLacksJID(t, ctx, keysStore, nonAD)

	if _, ok := manager.GetDevice(slotID); !ok {
		t.Fatal("expected slot to be kept after stale-id logout")
	}
	if got := inst.JID(); got != "" {
		t.Fatalf("expected instance JID cleared after stale-id logout, got %q", got)
	}
	if len(storage.savedRecords) == 0 {
		t.Fatal("expected SaveDeviceRecord to persist the kept slot")
	}
	last := storage.savedRecords[len(storage.savedRecords)-1]
	if last.DeviceID != slotID || last.JID != "" {
		t.Fatalf("expected persisted slot %s with empty JID, got %s / %q", slotID, last.DeviceID, last.JID)
	}
}

// Scenario: explicit logout for a paired client that is currently DISCONNECTED.
// IsLoggedIn() is only true while connected, so gating the unlink on it silently skips
// the remove-companion-device IQ and the phone keeps showing the linked device forever
// (the local session is deleted, so it can never reconnect to unlink later). The unlink
// must be ATTEMPTED whenever the client is paired (Store.ID set); failure stays
// best-effort and must not fail the local keep-slot cleanup.
func TestLogoutDeviceKeepSlot_AttemptsUnlinkWhenPairedButDisconnected(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281999999997", types.WhatsAppDomain, 15)
	nonAD := adJID.ToNonAD().String()
	dev := newTestStoreDevice(primaryStore, adJID, "primary")
	if err := dev.Save(ctx); err != nil {
		t.Fatalf("save primary device: %v", err)
	}

	// Real paired client (Store.ID set) that is not connected.
	cli := whatsmeow.NewClient(dev, nil)

	storage := &keepSlotStubStorage{}
	manager := NewDeviceManager(primaryStore, nil, storage)

	const slotID = "slot-uuid-4"
	inst := &DeviceInstance{id: slotID, jid: nonAD, displayName: "tIAtendo", client: cli, createdAt: time.Now()}
	manager.devices[slotID] = inst

	hook := logrustest.NewLocal(logrus.StandardLogger())
	defer hook.Reset()

	if err := manager.LogoutDeviceKeepSlot(ctx, slotID); err != nil {
		t.Fatalf("LogoutDeviceKeepSlot returned error: %v", err)
	}

	// The unlink attempt fails offline (best-effort), which is observable as the
	// remote-unlink warning. No warning means the attempt was skipped entirely.
	attempted := false
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "remote unlink failed") {
			attempted = true
		}
	}
	if !attempted {
		t.Fatal("expected cli.Logout to be attempted for a paired-but-disconnected client")
	}

	// Local keep-slot cleanup must still have run.
	assertStoreLacksJID(t, ctx, primaryStore, nonAD)
	if _, ok := manager.GetDevice(slotID); !ok {
		t.Fatal("expected slot to be kept after logout")
	}
	if got := inst.JID(); got != "" {
		t.Fatalf("expected instance JID cleared after logout, got %q", got)
	}
}

// Remote logout (events.LoggedOut) keeps the slot: routing the SetOnLoggedOut callback
// through keepSlotLogout (the same behavior both the lazy EnsureClient path and the
// startup InitWaCLI path now use) deletes the whatsmeow rows by JID but preserves the
// slot, instead of deleting it via RemoveDevice (aldinokemal device_manager.go:581).
func TestRemoteLogoutCallback_KeepsSlot(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	keysStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281999999995", types.WhatsAppDomain, 13)
	nonAD := adJID.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID, "primary").Save(ctx); err != nil {
		t.Fatalf("save primary device: %v", err)
	}
	if err := newTestStoreDevice(keysStore, adJID, "keys").Save(ctx); err != nil {
		t.Fatalf("save keys device: %v", err)
	}

	storage := &keepSlotStubStorage{}
	manager := NewDeviceManager(primaryStore, keysStore, storage)

	inst := &DeviceInstance{id: nonAD, jid: nonAD, displayName: "tIAtendo", createdAt: time.Now()}
	manager.devices[nonAD] = inst

	// Wire the callback exactly as the manager does for remote logout.
	inst.SetOnLoggedOut(func(deviceID string) {
		if err := manager.keepSlotLogout(context.Background(), deviceID); err != nil {
			t.Errorf("keepSlotLogout in remote-logout callback: %v", err)
		}
	})

	inst.TriggerLoggedOut()

	if _, ok := manager.GetDevice(nonAD); !ok {
		t.Fatal("expected slot to be kept on remote logout, but it was removed")
	}
	assertStoreLacksJID(t, ctx, primaryStore, nonAD)
	assertStoreLacksJID(t, ctx, keysStore, nonAD)
}

// Core precision fix: when the AD JID is known, deleteStoreRowsForJID must remove ONLY that
// companion's row, leaving a sibling companion of the SAME number (different device suffix)
// untouched in both the primary and keys stores. This is the guarantee the whole AD-JID work
// exists for -- the old by-number delete would have destroyed the sibling's live session.
func TestDeleteStoreRowsForJID_ADJIDDeletesOnlyThatCompanion(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	keysStore := newTestSQLStore(t)

	const user = "6281888888881"
	adJID28 := types.NewADJID(user, types.WhatsAppDomain, 28)
	adJID32 := types.NewADJID(user, types.WhatsAppDomain, 32)
	nonAD := adJID32.ToNonAD().String()

	for _, c := range []*sqlstore.Container{primaryStore, keysStore} {
		if err := newTestStoreDevice(c, adJID28, "companion-28").Save(ctx); err != nil {
			t.Fatalf("save :28 device: %v", err)
		}
		if err := newTestStoreDevice(c, adJID32, "companion-32").Save(ctx); err != nil {
			t.Fatalf("save :32 device: %v", err)
		}
	}

	manager := NewDeviceManager(primaryStore, keysStore, &keepSlotStubStorage{})

	if err := manager.deleteStoreRowsForJID(ctx, adJID32.String(), nonAD); err != nil {
		t.Fatalf("deleteStoreRowsForJID returned error: %v", err)
	}

	// :32 gone from both containers; :28 (the sibling) still present in both.
	assertStoreLacksADJID(t, ctx, primaryStore, adJID32.String())
	assertStoreLacksADJID(t, ctx, keysStore, adJID32.String())
	assertStoreHasADJID(t, ctx, primaryStore, adJID28.String())
	assertStoreHasADJID(t, ctx, keysStore, adJID28.String())
}

// Legacy path (adJID == ""): safe to delete by number only when exactly one store row AND
// one slot claim it. Here a single store row and a single claiming slot make the mapping
// unambiguous, so the row is deleted -- preserving the original by-number behaviour.
func TestDeleteStoreRowsForJID_LegacySingleRowSingleSlotDeletes(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281888888882", types.WhatsAppDomain, 21)
	nonAD := adJID.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID, "primary").Save(ctx); err != nil {
		t.Fatalf("save device: %v", err)
	}

	storage := &keepSlotStubStorage{
		listRecords: []*domainChatStorage.DeviceRecord{{DeviceID: "slot-a", JID: nonAD}},
	}
	manager := NewDeviceManager(primaryStore, nil, storage)

	if err := manager.deleteStoreRowsForJID(ctx, "", nonAD); err != nil {
		t.Fatalf("deleteStoreRowsForJID returned error: %v", err)
	}
	assertStoreLacksJID(t, ctx, primaryStore, nonAD)
}

// Legacy path, council edge 4: TWO store rows carry the number and no AD JID is known.
// Even with a single claiming slot, the by-number branch cannot tell which companion row
// belongs to the slot, so it must refuse and delete nothing (both rows remain).
func TestDeleteStoreRowsForJID_LegacyMultipleRowsRefuses(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)

	const user = "6281888888883"
	adJID28 := types.NewADJID(user, types.WhatsAppDomain, 28)
	adJID32 := types.NewADJID(user, types.WhatsAppDomain, 32)
	nonAD := adJID28.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID28, "c28").Save(ctx); err != nil {
		t.Fatalf("save :28: %v", err)
	}
	if err := newTestStoreDevice(primaryStore, adJID32, "c32").Save(ctx); err != nil {
		t.Fatalf("save :32: %v", err)
	}

	// slotsClaiming == 1 (passes the up-front guard), but the per-container scan finds two
	// rows carrying the number -> the delete must refuse.
	storage := &keepSlotStubStorage{
		listRecords: []*domainChatStorage.DeviceRecord{{DeviceID: "slot-a", JID: nonAD}},
	}
	manager := NewDeviceManager(primaryStore, nil, storage)

	if err := manager.deleteStoreRowsForJID(ctx, "", nonAD); err != nil {
		t.Fatalf("deleteStoreRowsForJID returned error: %v", err)
	}
	assertStoreHasADJID(t, ctx, primaryStore, adJID28.String())
	assertStoreHasADJID(t, ctx, primaryStore, adJID32.String())
}

// Legacy path, council edge 4b (the important one): exactly ONE store row carries the number,
// but TWO slots still register it (a sibling companion was already evicted, only this live
// row remains). slotsClaiming > 1 must short-circuit the delete so logging out an already
// evicted slot cannot kill the surviving sibling's live session.
func TestDeleteStoreRowsForJID_LegacyOneRowButTwoSlotsRefuses(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281888888884", types.WhatsAppDomain, 32)
	nonAD := adJID.ToNonAD().String()
	if err := newTestStoreDevice(primaryStore, adJID, "notificador").Save(ctx); err != nil {
		t.Fatalf("save device: %v", err)
	}

	storage := &keepSlotStubStorage{
		listRecords: []*domainChatStorage.DeviceRecord{
			{DeviceID: "auth-slot", JID: nonAD},
			{DeviceID: "notificador-slot", JID: nonAD},
		},
	}
	manager := NewDeviceManager(primaryStore, nil, storage)

	if err := manager.deleteStoreRowsForJID(ctx, "", nonAD); err != nil {
		t.Fatalf("deleteStoreRowsForJID returned error: %v", err)
	}
	// The lone live row must survive because two slots still claim the number.
	assertStoreHasADJID(t, ctx, primaryStore, adJID.String())
}

// Council edge 4c, weaker (idempotency) variant: the existing harness builds real
// sqlstore.Container values whose GetAllDevices/DeleteDevice cannot be forced to fail, so a
// failing keys container cannot be injected. Instead we prove idempotency, which is the
// property the partial-failure recovery relies on: deleting the AD row removes it on the
// first call, and a SECOND call is a clean no-op (no panic, no error, primary stays clean).
func TestDeleteStoreRowsForJID_KeysFailureIsIdempotent(t *testing.T) {
	ctx := context.Background()
	primaryStore := newTestSQLStore(t)
	keysStore := newTestSQLStore(t)

	adJID := types.NewADJID("6281888888885", types.WhatsAppDomain, 32)
	nonAD := adJID.ToNonAD().String()
	for _, c := range []*sqlstore.Container{primaryStore, keysStore} {
		if err := newTestStoreDevice(c, adJID, "companion").Save(ctx); err != nil {
			t.Fatalf("save device: %v", err)
		}
	}

	manager := NewDeviceManager(primaryStore, keysStore, &keepSlotStubStorage{})

	if err := manager.deleteStoreRowsForJID(ctx, adJID.String(), nonAD); err != nil {
		t.Fatalf("first delete returned error: %v", err)
	}
	assertStoreLacksADJID(t, ctx, primaryStore, adJID.String())
	assertStoreLacksADJID(t, ctx, keysStore, adJID.String())

	if err := manager.deleteStoreRowsForJID(ctx, adJID.String(), nonAD); err != nil {
		t.Fatalf("second (idempotent) delete returned error: %v", err)
	}
	assertStoreLacksADJID(t, ctx, primaryStore, adJID.String())
	assertStoreLacksADJID(t, ctx, keysStore, adJID.String())
}

// Silent-data-loss fix: two slot records share a NonAD number but map to DIFFERENT
// companions (distinct AD JIDs :28 and :32). loadFromRegistry must keep BOTH in memory --
// the AD-JID-aware dedup keeps both records, and the AD-JID-aware existingByJID match must
// not collapse them (matching on the bare number would evict the first when the second
// loads). Neither DeviceRecord may be deleted from storage.
func TestLoadFromRegistryKeepsTwoADDistinctSlots(t *testing.T) {
	const user = "6281888888886"
	adJID28 := types.NewADJID(user, types.WhatsAppDomain, 28)
	adJID32 := types.NewADJID(user, types.WhatsAppDomain, 32)
	nonAD := adJID28.ToNonAD().String()

	storage := &keepSlotStubStorage{}
	manager := NewDeviceManager(nil, nil, storage)

	records := []*domainChatStorage.DeviceRecord{
		{DeviceID: "auth-slot", DisplayName: "Auth", JID: nonAD, DeviceJID: adJID28.String()},
		{DeviceID: "notificador-slot", DisplayName: "Notificador", JID: nonAD, DeviceJID: adJID32.String()},
	}

	manager.loadFromRegistry(records)

	if _, ok := manager.GetDevice("auth-slot"); !ok {
		t.Fatal("expected auth-slot (:28) to remain registered after loadFromRegistry")
	}
	if _, ok := manager.GetDevice("notificador-slot"); !ok {
		t.Fatal("expected notificador-slot (:32) to remain registered after loadFromRegistry")
	}

	// No record may be deleted: reconciliation drops in-memory duplicates only, never
	// persisted slots. keepSlotStubStorage records every DeleteDeviceRecord call.
	if len(storage.deletedRecords) != 0 {
		t.Fatalf("expected no DeleteDeviceRecord calls, got %v", storage.deletedRecords)
	}
}

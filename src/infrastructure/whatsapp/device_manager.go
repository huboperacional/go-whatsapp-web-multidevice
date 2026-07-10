package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/config"
	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	domainDevice "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/device"
	fiberUtils "github.com/gofiber/fiber/v2/utils"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// DeviceManager keeps a registry of active device instances.
type DeviceManager struct {
	mu       sync.RWMutex
	devices  map[string]*DeviceInstance
	store    *sqlstore.Container
	keys     *sqlstore.Container
	storage  domainChatStorage.IChatStorageRepository
	initted  bool
	initOnce sync.Once
}

func NewDeviceManager(store *sqlstore.Container, keys *sqlstore.Container, chatStorageRepo domainChatStorage.IChatStorageRepository) *DeviceManager {
	return &DeviceManager{
		devices: make(map[string]*DeviceInstance),
		store:   store,
		keys:    keys,
		storage: chatStorageRepo,
	}
}

func (m *DeviceManager) AddDevice(instance *DeviceInstance) {
	if instance == nil || instance.ID() == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.devices[instance.ID()] = instance

	// Persist registry entry if available
	if m.storage != nil {
		_ = m.storage.SaveDeviceRecord(&domainChatStorage.DeviceRecord{
			DeviceID:    instance.ID(),
			DisplayName: instance.DisplayName(),
			JID:         instance.JID(),
			DeviceJID:   instance.ADJID(),
			CreatedAt:   instance.CreatedAt(),
			UpdatedAt:   time.Now(),
		})
	}
}

func (m *DeviceManager) GetDevice(id string) (*DeviceInstance, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	instance, ok := m.devices[id]
	return instance, ok
}

func (m *DeviceManager) getDeviceByJID(jid string) (*DeviceInstance, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, inst := range m.devices {
		if inst != nil && inst.JID() == jid {
			return inst, true
		}
	}
	return nil, false
}

// IsHealthy returns true if the device manager is initialized and has a valid store connection.
// Note: This is a service initialization check, not a live connectivity check.
// Returning true indicates the internal store is ready, but does not guarantee
// that any WhatsApp device connections are currently active or authenticated.
func (m *DeviceManager) IsHealthy() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store != nil
}

// DefaultDevice returns the only registered device when running in single-device mode.
func (m *DeviceManager) DefaultDevice() *DeviceInstance {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.devices) != 1 {
		return nil
	}

	for _, inst := range m.devices {
		return inst
	}

	return nil
}

// ResolveDevice attempts to locate a device by ID or falls back to the default/only device.
// It returns the resolved instance, the ID used, or an error when no suitable device is found.
func (m *DeviceManager) ResolveDevice(deviceID string) (*DeviceInstance, string, error) {
	if m == nil {
		return nil, "", fmt.Errorf("device manager not initialized")
	}

	trimmedID := strings.TrimSpace(deviceID)
	if trimmedID != "" {
		if inst, ok := m.GetDevice(trimmedID); ok && inst != nil {
			return inst, trimmedID, nil
		}
		if inst, ok := m.getDeviceByJID(trimmedID); ok && inst != nil {
			return inst, inst.ID(), nil
		}
		return nil, trimmedID, fmt.Errorf("device %s not found", trimmedID)
	}

	if inst := m.DefaultDevice(); inst != nil {
		return inst, inst.ID(), nil
	}

	return nil, "", fmt.Errorf("device id is required")
}

func (m *DeviceManager) RemoveDevice(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.devices, id)

	if m.storage != nil && strings.TrimSpace(id) != "" {
		_ = m.storage.DeleteDeviceRecord(id)
	}
}

// deleteStoreRowsForJID removes the whatsmeow session rows for a slot's companion.
//
// When adJID is known it identifies exactly one companion, so we delete precisely
// that row (a number can host several companions -- deleting by number alone can
// destroy a different, live slot's session).
//
// When adJID is empty (legacy rows written before the AD JID was tracked) we can
// only match by number. That is safe ONLY when the mapping is unambiguous: exactly
// one store row carries the number AND exactly one slot claims it. Otherwise we
// delete nothing and log loudly -- never guess. Run the prune-devices command to
// clean such numbers up once the AD JIDs have been backfilled.
//
// An empty nonADJID is a no-op (a slot that was never paired has no rows).
func (m *DeviceManager) deleteStoreRowsForJID(ctx context.Context, adJID, nonADJID string) error {
	if strings.TrimSpace(nonADJID) == "" && strings.TrimSpace(adJID) == "" {
		return nil
	}

	// Legacy ambiguity guard: with no AD JID we can only match by number, so resolve how
	// many slots claim this number ONCE, up front. If more than one slot claims it we must
	// not guess which companion row belongs to which slot -- deleting the wrong one would
	// destroy a different, live session. Computed only on the legacy path so the hot
	// (AD JID known) path never pays the ListDeviceRecords cost.
	slotsClaiming := 0
	if adJID == "" {
		if m.storage != nil {
			records, err := m.storage.ListDeviceRecords()
			if err != nil {
				logrus.WithError(err).Errorf("[DEVICE_MANAGER] cannot resolve slot ownership for number %s; refusing to delete store rows", nonADJID)
				return err
			}
			for _, rec := range records {
				if rec != nil && rec.JID == nonADJID {
					slotsClaiming++
				}
			}
		}
		if slotsClaiming > 1 {
			logrus.Errorf("[DEVICE_MANAGER] %d slots claim number %s and no AD JID is known; refusing to delete any whatsmeow rows -- run prune-devices after the AD JIDs are backfilled", slotsClaiming, nonADJID)
			return nil
		}
	}

	// There is no cross-container transaction: a failure deleting from one container can
	// leave a row in the other. That is acceptable because the function is idempotent -- a
	// retry, or prune-devices, cleans the remainder, and the per-container log line names
	// which container was left dirty.
	var firstErr error
	deleteFrom := func(container *sqlstore.Container, label string) {
		if container == nil {
			return
		}
		devices, err := container.GetAllDevices(ctx)
		if err != nil {
			logrus.WithError(err).Warnf("[DEVICE_MANAGER] failed to enumerate %s devices for jid %s", label, nonADJID)
			firstErr = errors.Join(firstErr, err)
			return
		}

		if adJID != "" {
			// AD JID known: delete precisely the matching companion row(s). In practice
			// exactly one matches, but do not rely on it -- delete every exact match.
			for _, dev := range devices {
				if dev == nil || dev.ID == nil {
					continue
				}
				if dev.ID.String() != adJID {
					continue
				}
				if err := container.DeleteDevice(ctx, dev); err != nil {
					logrus.WithError(err).Warnf("[DEVICE_MANAGER] failed to delete ad jid %s from %s store", adJID, label)
					firstErr = errors.Join(firstErr, err)
				}
			}
			return
		}

		// Legacy path: match by number. Collect every row carrying this number first;
		// only delete when exactly one row AND one slot claim it (both required -- a lone
		// surviving row may belong to a different, live slot whose sibling companion was
		// already evicted).
		var matches []*store.Device
		for _, dev := range devices {
			if dev == nil || dev.ID == nil {
				continue
			}
			if dev.ID.ToNonAD().String() == nonADJID {
				matches = append(matches, dev)
			}
		}
		switch {
		case len(matches) == 0:
			// nothing to delete
		case len(matches) == 1 && slotsClaiming <= 1:
			if err := container.DeleteDevice(ctx, matches[0]); err != nil {
				logrus.WithError(err).Warnf("[DEVICE_MANAGER] failed to delete jid %s from %s store", nonADJID, label)
				firstErr = errors.Join(firstErr, err)
			}
		default: // len(matches) > 1
			logrus.Errorf("[DEVICE_MANAGER] %d store rows carry number %s in %s store and no AD JID is known; refusing to delete any -- run prune-devices after the AD JIDs are backfilled", len(matches), nonADJID, label)
		}
	}

	deleteFrom(m.store, "primary")
	if m.keys != nil && m.keys != m.store {
		deleteFrom(m.keys, "keys")
	}
	return firstErr
}

// PurgeDevice cleanly logs out a device, removes its persisted records (store/keys),
// deletes its chatstorage data, and removes it from the in-memory registry.
func (m *DeviceManager) PurgeDevice(ctx context.Context, deviceID string) error {
	if deviceID == "" {
		return fmt.Errorf("device id is required")
	}

	var firstErr error
	recordErr := func(err error) {
		if err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}

	// Resolve the device's WhatsApp JID (and full AD JID) before tearing anything down so
	// we can delete its whatsmeow store rows even when no live client is attached. The AD
	// JID must be read here, before any reset, because it pins the exact companion row.
	var jid string
	var adJID string
	if inst, ok := m.GetDevice(deviceID); ok && inst != nil {
		jid = inst.JID()
		adJID = inst.ADJID()
		if cli := inst.GetClient(); cli != nil {
			// The WhatsApp unlink is best-effort: a dead/expired session may fail
			// here, but that must not block local cleanup or fail the purge.
			if err := cli.Logout(ctx); err != nil {
				logrus.WithError(err).Warnf("[DEVICE_MANAGER] remote unlink failed for device %s (best-effort)", deviceID)
			}
			cli.Disconnect()
		}
	}

	// Delete chatstorage data for this device (local cleanup — surfaced on failure).
	if m.storage != nil {
		if err := m.storage.DeleteDeviceData(deviceID); err != nil {
			logrus.WithError(err).Warnf("[DEVICE_MANAGER] failed to delete chatstorage for device %s", deviceID)
			recordErr(err)
		}
	}

	// Delete whatsmeow store/keys rows (local cleanup — surfaced on failure). Prefer the
	// full AD JID so we delete only this slot's companion, never a sibling's live session.
	recordErr(m.deleteStoreRowsForJID(ctx, adJID, jid))

	// Remove from registry last
	m.RemoveDevice(deviceID)
	return firstErr
}

// LogoutDeviceKeepSlot logs the device out of WhatsApp (clearing its session/keys)
// but PRESERVES the device slot in the registry, so it keeps its id and display name
// and can be re-paired later under the same id. Unlike PurgeDevice, it does not remove
// the device record. Removing the slot entirely is the job of RemoveDevice (DELETE).
func (m *DeviceManager) LogoutDeviceKeepSlot(ctx context.Context, deviceID string) error {
	if deviceID == "" {
		return fmt.Errorf("device id is required")
	}

	inst, ok := m.GetDevice(deviceID)
	if !ok || inst == nil {
		return fmt.Errorf("device %s not found", deviceID)
	}

	if cli := inst.GetClient(); cli != nil {
		// Attempt the unlink whenever the client is paired (Store.ID set), not only when
		// IsLoggedIn: that is true only while connected, and skipping the attempt for a
		// momentarily-offline client would leave the phone showing the linked device
		// forever (the local session is deleted below, so it can never unlink later).
		// The WhatsApp unlink is best-effort: a dead/expired session may fail
		// here, but that must not block the local keep-slot cleanup below.
		if cli.Store != nil && cli.Store.ID != nil {
			if err := cli.Logout(ctx); err != nil {
				logrus.WithError(err).Warnf("[DEVICE_MANAGER] remote unlink failed for device %s (best-effort)", deviceID)
			}
		}
		cli.Disconnect()
	}

	return m.keepSlotLogout(ctx, deviceID)
}

// keepSlotLogout is the shared "logout but keep the slot" cleanup used by both explicit
// logout and the remote LoggedOut callbacks. It deletes the device's whatsmeow store
// rows (by JID, resolved before the reset clears it) and resets the in-memory client +
// persisted JID, keeping the slot (id + display name) for re-pairing. It does NOT call
// cli.Logout — explicit logout handles the unlink before delegating here, and a remote
// LoggedOut has already been unlinked on the phone.
func (m *DeviceManager) keepSlotLogout(ctx context.Context, deviceID string) error {
	inst, ok := m.GetDevice(deviceID)
	if !ok || inst == nil {
		// The remote-logout callback can hold a stale id: InitWaCLI keys its instance by
		// the AD JID string, but loadFromRegistry may replace it with a registry slot
		// keyed by uuid (instances store NonAD JIDs). Fall back to JID resolution so the
		// cleanup still lands on the surviving slot instead of leaving a stale JID and
		// an orphan keys-container row.
		if parsed, err := types.ParseJID(deviceID); err == nil && parsed.User != "" {
			inst, ok = m.getDeviceByJID(parsed.ToNonAD().String())
		}
		if !ok || inst == nil {
			return fmt.Errorf("device %s not found", deviceID)
		}
		deviceID = inst.ID()
	}

	// Resolve the JID and full AD JID before resetDeviceKeepSlot clears them, so we can
	// delete the stored whatsmeow rows even when no live client is attached (slot loaded
	// from storage). Both are read off the (possibly re-resolved above) inst. Always
	// delete (don't rely on cli.Logout having done it): an orphan row would otherwise get
	// matched back on restart. Idempotent when the row is already gone. The AD JID, when
	// known, pins the exact companion so a sibling slot sharing the number is untouched.
	jid := inst.JID()
	adJID := inst.ADJID()

	var firstErr error
	firstErr = errors.Join(firstErr, m.deleteStoreRowsForJID(ctx, adJID, jid))
	firstErr = errors.Join(firstErr, m.resetDeviceKeepSlot(deviceID))
	return firstErr
}

// resetDeviceKeepSlot detaches the in-memory client and clears the persisted session
// identity (jid) while keeping the device slot (id + display name) in both the
// in-memory registry and the persisted device registry. EnsureClient rebuilds a
// fresh client on the next login, so the slot can be re-paired under the same id.
func (m *DeviceManager) resetDeviceKeepSlot(deviceID string) error {
	inst, ok := m.GetDevice(deviceID)
	if !ok || inst == nil {
		return fmt.Errorf("device %s not found", deviceID)
	}
	inst.ResetClient()

	if m.storage != nil && strings.TrimSpace(deviceID) != "" {
		if err := m.storage.SaveDeviceRecord(&domainChatStorage.DeviceRecord{
			DeviceID:    deviceID,
			DisplayName: inst.DisplayName(),
			JID:         "",
			// On logout the slot no longer maps to any companion, so clear the
			// AD JID together with the JID.
			DeviceJID: "",
			CreatedAt: inst.CreatedAt(),
			UpdatedAt: time.Now(),
		}); err != nil {
			return fmt.Errorf("persist logged-out device %s: %w", deviceID, err)
		}
	}
	return nil
}

// CreateDevice registers a new device placeholder so routes can be scoped strictly by device_id.
func (m *DeviceManager) CreateDevice(ctx context.Context, requestedID string) (*DeviceInstance, error) {
	if m == nil {
		return nil, fmt.Errorf("device manager not initialized")
	}

	id := requestedID
	if id == "" {
		id = fiberUtils.UUID()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.devices[id]; exists {
		return nil, fmt.Errorf("device %s already exists", id)
	}

	instance := NewDeviceInstance(id, nil, newDeviceChatStorage(id, m.storage))
	m.devices[id] = instance

	if m.storage != nil {
		if err := m.storage.SaveDeviceRecord(&domainChatStorage.DeviceRecord{
			DeviceID:    id,
			DisplayName: instance.DisplayName(),
			JID:         instance.JID(),
			DeviceJID:   instance.ADJID(),
			CreatedAt:   instance.CreatedAt(),
			UpdatedAt:   instance.CreatedAt(),
		}); err != nil {
			logrus.WithError(err).Warnf("[DEVICE_MANAGER] failed to persist device %s", id)
		}
	}

	logrus.WithContext(ctx).Infof("[DEVICE_MANAGER] created device placeholder %s", id)
	return instance, nil
}

func (m *DeviceManager) ListDevices() []*DeviceInstance {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*DeviceInstance, 0, len(m.devices))
	for _, instance := range m.devices {
		result = append(result, instance)
	}

	// Sort by CreatedAt ascending (oldest first) for stable UI ordering.
	// Use ID as tie-breaker when CreatedAt is equal.
	slices.SortFunc(result, func(a, b *DeviceInstance) int {
		if cmp := a.CreatedAt().Compare(b.CreatedAt()); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID(), b.ID())
	})

	return result
}

// LoadExistingDevices registers existing device records in the store container without connecting them.
// This keeps the registry aware of all device IDs even before their clients are initialized.
func (m *DeviceManager) LoadExistingDevices(ctx context.Context) error {
	if m == nil || m.store == nil {
		return fmt.Errorf("device manager not initialized")
	}

	m.initOnce.Do(func() {
		m.initted = true
	})

	// Load from persisted registry
	if m.storage != nil {
		records, err := m.storage.ListDeviceRecords()
		if err != nil {
			logrus.WithError(err).Warn("[DEVICE_MANAGER] failed to load device registry")
		} else {
			logrus.Infof("[DEVICE_MANAGER] discovered %d device records in registry", len(records))
			m.loadFromRegistry(records)
		}
	}

	// Load from WhatsMeow store
	devices, err := m.store.GetAllDevices(ctx)
	if err != nil {
		return err
	}

	logrus.Infof("[DEVICE_MANAGER] discovered %d device records in store", len(devices))

	// A number can host several companions. Count them up front so we only ever bind a
	// slot to a store row when the mapping is unambiguous -- binding the wrong companion
	// would hand two slots the same session.
	rowsPerNumber := make(map[string]int, len(devices))
	for _, dev := range devices {
		if dev == nil || dev.ID == nil {
			continue
		}
		rowsPerNumber[dev.ID.ToNonAD().String()]++
	}

	for _, dev := range devices {
		if dev == nil || dev.ID == nil {
			continue
		}
		// Use NonAD JID to match with devices table which stores NonAD format
		jid := dev.ID.ToNonAD().String()

		// Check if device already exists by ID or JID
		m.mu.RLock()
		existingByID := m.devices[jid]
		var matchedDevice *DeviceInstance
		var orphanDevice *DeviceInstance
		for _, inst := range m.devices {
			if inst.JID() == jid {
				matchedDevice = inst
				break
			}
			if inst.JID() == "" && orphanDevice == nil {
				orphanDevice = inst
			}
		}
		m.mu.RUnlock()

		// Skip if already matched
		if existingByID != nil {
			m.applyStoreJID(existingByID, jid, dev.ID.String())
			continue
		}
		if matchedDevice != nil {
			// Legacy slot (no AD JID persisted yet): adopt this row's AD JID so the slot keeps
			// driving its existing companion instead of minting a new one on the next connect.
			// Only safe when exactly one row carries the number.
			if matchedDevice.ADJID() == "" {
				if rowsPerNumber[jid] == 1 {
					m.applyStoreJID(matchedDevice, jid, dev.ID.String())
				} else {
					logrus.Warnf("[DEVICE_MANAGER] %d companions carry number %s; cannot infer which belongs to slot %s -- it will require a fresh pairing (run prune-devices to clean up)", rowsPerNumber[jid], jid, matchedDevice.ID())
				}
			}
			continue
		}

		// Match orphaned device with this JID
		if orphanDevice != nil {
			logrus.Infof("[DEVICE_MANAGER] matching orphaned device %s with JID %s", orphanDevice.ID(), jid)
			m.applyStoreJID(orphanDevice, jid, dev.ID.String())
			if m.storage != nil {
				_ = m.storage.SaveDeviceRecord(&domainChatStorage.DeviceRecord{
					DeviceID:  orphanDevice.ID(),
					JID:       jid,
					DeviceJID: dev.ID.String(),
				})
			}
			continue
		}

		// Create new device instance for a store row that no slot claims. By default we
		// do NOT: adopting it registers an instance that the auto-connect loop then dials
		// forever against a dead session (endless websocket close 1005 churn). Opt back
		// into the legacy behaviour with WHATSAPP_ADOPT_ORPHAN_STORE_DEVICES=true.
		if !config.WhatsappAdoptOrphanStoreDevices {
			logrus.Warnf("[DEVICE_MANAGER] store row %s is not claimed by any slot; skipping adoption (set WHATSAPP_ADOPT_ORPHAN_STORE_DEVICES=true to restore the old behaviour). Run prune-devices to clean it up.", jid)
			continue
		}
		instance := NewDeviceInstance(jid, nil, newDeviceChatStorage(jid, m.storage))
		instance.SetState(domainDevice.DeviceStateDisconnected)
		m.applyStoreJID(instance, jid, dev.ID.String())
		m.AddDevice(instance)
	}

	return nil
}

func (m *DeviceManager) applyStoreJID(instance *DeviceInstance, jid, adJID string) {
	if instance == nil || jid == "" {
		return
	}
	instance.mu.Lock()
	instance.jid = jid
	instance.adJID = adJID
	instance.mu.Unlock()
	instance.SetChatStorage(newDeviceChatStorage(jid, m.storage))
}

// loadFromRegistry loads devices from the registry, handling deduplication.
func (m *DeviceManager) loadFromRegistry(records []*domainChatStorage.DeviceRecord) {
	// Collect JIDs from manual devices (device_id doesn't contain @)
	manualDeviceJIDs := make(map[string]bool)
	for _, rec := range records {
		if rec == nil || strings.TrimSpace(rec.DeviceID) == "" {
			continue
		}
		if !strings.Contains(rec.DeviceID, "@") && rec.JID != "" {
			manualDeviceJIDs[rec.JID] = true
		}
	}

	// Load devices, skipping (never deleting) auto-created and duplicate records.
	// Dedup keys on the full AD JID when known so two genuine companions of the same
	// number both load; only un-backfilled legacy rows fall back to NonAD dedup.
	seenADJIDs := make(map[string]bool)
	seenNonADJIDs := make(map[string]bool)
	for _, rec := range records {
		if rec == nil || strings.TrimSpace(rec.DeviceID) == "" {
			continue
		}

		// Skip (do NOT delete) auto-created devices when a manual slot already claims
		// the same JID. Deleting a persisted slot record during startup reconciliation
		// is silent data loss; record deletion belongs to RemoveDevice/PurgeDevice only.
		isAutoCreated := strings.Contains(rec.DeviceID, "@")
		if isAutoCreated && manualDeviceJIDs[rec.DeviceID] {
			logrus.Warnf("[DEVICE_MANAGER] skipping (not deleting) auto-created device %s: a manual slot already claims its JID", rec.DeviceID)
			continue
		}

		// Skip duplicates. Prefer the full AD JID: a true duplicate is only the same
		// companion appearing twice. Legacy records with no AD JID fall back to NonAD
		// dedup, preserving today's behaviour until backfill disambiguates them.
		if rec.DeviceJID != "" {
			if seenADJIDs[rec.DeviceJID] {
				logrus.Warnf("[DEVICE_MANAGER] skipping (not deleting) duplicate AD JID device %s", rec.DeviceID)
				continue
			}
			seenADJIDs[rec.DeviceJID] = true
		} else if rec.JID != "" {
			if seenNonADJIDs[rec.JID] {
				logrus.Warnf("[DEVICE_MANAGER] skipping (not deleting) device %s: its number collides with another un-backfilled record; backfilling the AD JID will disambiguate it", rec.DeviceID)
				continue
			}
			seenNonADJIDs[rec.JID] = true
		}

		// Check if the same session already exists in memory under a different id (e.g.
		// bootstrapped by InitWaCLI) so we can replace it with this registry slot and
		// transfer its live client. Match on the full AD JID when known -- matching on the
		// bare number would collapse two distinct companions of the same number into one.
		// Legacy records with no AD JID fall back to the number (unambiguous only until
		// backfill; the same limitation the dedup above carries).
		m.mu.RLock()
		var existingByJID *DeviceInstance
		for id, inst := range m.devices {
			if id == rec.DeviceID {
				continue
			}
			if rec.DeviceJID != "" {
				if inst.ADJID() == rec.DeviceJID {
					existingByJID = inst
					break
				}
				continue
			}
			if rec.JID != "" && inst.JID() == rec.JID {
				existingByJID = inst
				break
			}
		}
		m.mu.RUnlock()

		// If device with matching JID exists, remove it and use the registry device
		if existingByJID != nil {
			m.mu.Lock()
			delete(m.devices, existingByJID.ID())
			m.mu.Unlock()
			logrus.Infof("[DEVICE_MANAGER] replacing in-memory device %s with registry device %s", existingByJID.ID(), rec.DeviceID)
		}

		// Create device instance
		storageDeviceID := rec.DeviceID
		if rec.JID != "" {
			storageDeviceID = rec.JID
		}
		instance := NewDeviceInstance(rec.DeviceID, nil, newDeviceChatStorage(storageDeviceID, m.storage))
		instance.SetState(domainDevice.DeviceStateDisconnected)
		instance.displayName = rec.DisplayName
		instance.jid = rec.JID
		instance.adJID = rec.DeviceJID

		// If we had an existing device with client, transfer the client
		if existingByJID != nil {
			if client := existingByJID.GetClient(); client != nil {
				instance.SetClient(client)
				instance.UpdateStateFromClient()
			}
		}

		m.AddDevice(instance)
	}
}

// EnsureDefault registers the current global client as the default device if present.
// It checks both by device ID and by JID to avoid creating duplicates.
func (m *DeviceManager) EnsureDefault(client *DeviceInstance) {
	if client == nil || client.ID() == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if device exists by ID
	if _, ok := m.devices[client.ID()]; ok {
		return
	}

	// Check if any existing device has matching JID
	clientJID := client.JID()
	if clientJID != "" {
		for _, inst := range m.devices {
			if inst.JID() == clientJID {
				// Update existing device with the new client
				inst.SetClient(client.GetClient())
				return
			}
		}
	}

	m.devices[client.ID()] = client
}

// EnsureClient returns a device instance with an initialized WhatsApp client.
// It lazily creates the underlying store device and registers event handlers.
func (m *DeviceManager) EnsureClient(ctx context.Context, deviceID string) (*DeviceInstance, error) {
	if m == nil {
		return nil, fmt.Errorf("device manager not initialized")
	}

	inst := m.ensureInstance(deviceID)
	if existing := inst.GetClient(); existing != nil {
		inst.UpdateStateFromClient()
		return inst, nil
	}

	storeDevice, err := m.getOrCreateStoreDevice(ctx, deviceID)
	if err != nil {
		return nil, err
	}

	configureDeviceProps()

	if err := m.configureKeysStore(ctx, storeDevice); err != nil {
		return nil, fmt.Errorf("failed to configure keys store: %w", err)
	}

	baseLogger := waLog.Stdout(fmt.Sprintf("Client-%s", deviceID), config.WhatsappLogLevel, true)
	client := whatsmeow.NewClient(storeDevice, newFilteredLogger(baseLogger))
	if proxyURL := config.WhatsappProxy; proxyURL != "" {
		if err := client.SetProxyAddress(proxyURL); err != nil {
			baseLogger.Errorf("failed to apply WHATSAPP_PROXY=%q for device %s: %v", redactProxyURL(proxyURL), deviceID, err)
		} else {
			baseLogger.Infof("applied outbound proxy from WHATSAPP_PROXY for device %s", deviceID)
		}
	}
	client.EnableAutoReconnect = true
	client.AutoTrustIdentity = true

	repo := inst.GetChatStorage()
	if repo == nil {
		repo = newDeviceChatStorage(deviceID, m.storage)
		inst.SetChatStorage(repo)
	}

	client.AddEventHandler(func(rawEvt any) {
		handler(ctx, inst, rawEvt)
	})

	inst.SetOnLoggedOut(func(deviceID string) {
		// On remote logout (device unlinked from the phone) keep the slot so it can
		// be re-paired under the same id, matching the explicit logout semantics.
		// Use a fresh context: the original request ctx may already be cancelled.
		if err := m.keepSlotLogout(context.Background(), deviceID); err != nil {
			logrus.WithError(err).Warnf("[REMOTE_LOGOUT] keep-slot cleanup failed for %s", deviceID)
		}
	})

	inst.SetClient(client)
	inst.UpdateStateFromClient()

	return inst, nil
}

func (m *DeviceManager) ensureInstance(deviceID string) *DeviceInstance {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check by device ID first
	if inst, ok := m.devices[deviceID]; ok {
		if inst.GetChatStorage() == nil {
			storageDeviceID := inst.JID()
			if storageDeviceID == "" {
				storageDeviceID = deviceID
			}
			inst.SetChatStorage(newDeviceChatStorage(storageDeviceID, m.storage))
		}
		return inst
	}

	// Check if any existing device has this as its JID (deviceID might be a JID)
	for _, inst := range m.devices {
		if inst.JID() == deviceID {
			if inst.GetChatStorage() == nil {
				storageDeviceID := inst.JID()
				if storageDeviceID == "" {
					storageDeviceID = inst.ID()
				}
				inst.SetChatStorage(newDeviceChatStorage(storageDeviceID, m.storage))
			}
			return inst
		}
	}

	inst := NewDeviceInstance(deviceID, nil, newDeviceChatStorage(deviceID, m.storage))
	m.devices[deviceID] = inst
	return inst
}

func (m *DeviceManager) getOrCreateStoreDevice(ctx context.Context, deviceID string) (*store.Device, error) {
	if m.store == nil {
		return nil, fmt.Errorf("store container is nil")
	}

	// Reuse an existing session row ONLY when we can identify the exact companion by
	// its full AD JID. Resolving by the bare number would risk adopting a sibling
	// slot's live session (a number can host several companions).
	if deviceID != "" {
		// Case 1: deviceID itself parses as an AD JID (has a non-zero device/agent
		// part, so parsed.String() differs from its NonAD form).
		if parsed, err := types.ParseJID(deviceID); err == nil && parsed.String() != parsed.ToNonAD().String() {
			if dev, err := findStoreDeviceByADJID(ctx, m.store, parsed); err != nil {
				return nil, err
			} else if dev != nil {
				return dev, nil
			}
		}

		// Case 2: deviceID is an opaque slot id -- look up the in-memory instance and
		// use its recorded AD JID (never its NonAD JID).
		m.mu.RLock()
		var instADJID string
		var instNonAD string
		if inst, ok := m.devices[deviceID]; ok {
			instADJID = inst.ADJID()
			instNonAD = inst.JID()
		}
		m.mu.RUnlock()

		if instADJID != "" {
			if jid, err := types.ParseJID(instADJID); err == nil {
				if dev, err := findStoreDeviceByADJID(ctx, m.store, jid); err != nil {
					return nil, err
				} else if dev != nil {
					return dev, nil
				}
			}
		}

		// Last resort before minting: resolve by the best-known bare number, but only
		// when exactly one store row carries it. Prefer the number parsed from deviceID
		// (when it is itself a JID); otherwise fall back to the in-memory instance's JID.
		nonAD := instNonAD
		if parsed, err := types.ParseJID(deviceID); err == nil && parsed.User != "" {
			nonAD = parsed.ToNonAD().String()
		}
		if strings.TrimSpace(nonAD) != "" {
			if dev, err := findStoreDeviceByUniqueNumber(ctx, m.store, nonAD); err != nil {
				return nil, err
			} else if dev != nil {
				return dev, nil
			}
		}
	}

	// Unknown or ambiguous mapping: deliberately mint a fresh companion (a new QR)
	// rather than adopting a sibling slot's session row.
	return m.store.NewDevice(), nil
}

func (m *DeviceManager) configureKeysStore(ctx context.Context, device *store.Device) error {
	if m.keys == nil || device == nil || device.ID == nil {
		return nil
	}

	innerStore := sqlstore.NewSQLStore(m.keys, *device.ID)
	syncKeysDevice(ctx, m.store, m.keys, *device.ID)

	applyKeyCacheStore(device, innerStore)
	return nil
}

// findStoreDeviceByADJID returns the whatsmeow session row for exactly one companion.
//
// It matches on the full AD JID. Matching on the bare number is unsafe: a number can
// host several companions, and adopting a row that belongs to a different slot would
// hand two slots the same session.
func findStoreDeviceByADJID(ctx context.Context, container *sqlstore.Container, adJID types.JID) (*store.Device, error) {
	if container == nil || adJID.IsEmpty() {
		return nil, nil
	}

	if dev, err := container.GetDevice(ctx, adJID); err != nil {
		return nil, err
	} else if dev != nil {
		return dev, nil
	}
	return nil, nil
}

// findStoreDeviceByUniqueNumber resolves a companion from a bare (NonAD) number.
//
// A number can host several companions, so this only succeeds when exactly one store
// row carries it. When several do, the mapping is ambiguous and we return nil rather
// than adopt a row that may belong to a different, live slot.
func findStoreDeviceByUniqueNumber(ctx context.Context, container *sqlstore.Container, nonADJID string) (*store.Device, error) {
	if container == nil || strings.TrimSpace(nonADJID) == "" {
		return nil, nil
	}
	devices, err := container.GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	var match *store.Device
	for _, dev := range devices {
		if dev == nil || dev.ID == nil {
			continue
		}
		if dev.ID.ToNonAD().String() != nonADJID {
			continue
		}
		if match != nil {
			logrus.Warnf("[DEVICE_MANAGER] several companions carry number %s; refusing to guess which one to reuse -- a fresh pairing will be created", nonADJID)
			return nil, nil
		}
		match = dev
	}
	return match, nil
}

func configureDeviceProps() {
	osName := fmt.Sprintf("%s %s", config.AppOs, config.AppVersion)
	store.DeviceProps.PlatformType = &config.AppPlatform
	store.DeviceProps.Os = &osName
}

// StoreInfo returns configured store URIs for observability.
func (m *DeviceManager) StoreInfo() (dbURI, keysURI string) {
	if m == nil {
		return "", ""
	}
	return config.DBURI, config.DBKeysURI
}

// GetStorage returns the chat storage repository.
func (m *DeviceManager) GetStorage() domainChatStorage.IChatStorageRepository {
	if m == nil {
		return nil
	}
	return m.storage
}

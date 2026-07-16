package whatsapp

import (
	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

// GetClient returns the current global client instance (alias for GetGlobalClient)
func GetClient() *whatsmeow.Client {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()
	return cli
}

func getStoreContainers() (*sqlstore.Container, *sqlstore.Container) {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()
	return db, keysDB
}

// GetStoreContainers exposes the initialized whatsmeow store containers (primary, keys)
// for maintenance commands (e.g. prune-devices) that need direct row-level access to the
// same session DBs the service uses, without constructing a live client or dialing WhatsApp.
// The keys container is nil when no separate keys DB is configured.
func GetStoreContainers() (*sqlstore.Container, *sqlstore.Container) {
	return getStoreContainers()
}

// InitializeDeviceManager creates the global DeviceManager if it doesn't exist.
func InitializeDeviceManager(storeContainer, keysStoreContainer *sqlstore.Container, chatStorageRepo domainChatStorage.IChatStorageRepository) *DeviceManager {
	globalStateMu.Lock()
	defer globalStateMu.Unlock()
	if deviceManager == nil {
		deviceManager = NewDeviceManager(storeContainer, keysStoreContainer, chatStorageRepo)
	}
	return deviceManager
}

// GetDeviceManager returns the global DeviceManager.
func GetDeviceManager() *DeviceManager {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()
	return deviceManager
}

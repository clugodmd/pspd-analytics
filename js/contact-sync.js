/**
 * Contact Sync Module for PSPD Call Sheets
 * Provides centralized contact tracking across workstations
 *
 * Usage:
 *   ContactSync.init('https://your-azure-function-url/api/contact-log', onUpdateCallback);
 *   ContactSync.logContact({ row_id, household_id, phone, contact_name, contact_type, ... });
 *   ContactSync.startPolling();
 */

const ContactSync = (() => {
  // Private state
  let apiUrl = '';
  let pollInterval = 20000; // 20 seconds
  let workstationId = '';
  let lastSync = null;
  let pollingHandle = null;
  let onUpdateCallback = null;
  let contacts = new Map(); // row_id -> array of contact entries
  let connectionStatus = 'offline'; // 'online', 'syncing', 'offline'
  let useFallback = false; // Use localStorage only mode

  const STORAGE_KEYS = {
    WORKSTATION_ID: 'pspd_workstation_id',
    LOCAL_CONTACTS: 'pspd_local_contacts',
    LAST_SYNC: 'pspd_last_sync'
  };

  const STATUS_INDICATORS = {
    online: { color: '#22c55e', label: 'Synced' },
    syncing: { color: '#eab308', label: 'Syncing' },
    offline: { color: '#ef4444', label: 'Offline (Local)' }
  };

  /**
   * Generate a unique workstation ID
   * Format: {office}-{random4chars}
   */
  function generateWorkstationId() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let random = '';
    for (let i = 0; i < 4; i++) {
      random += chars.charAt(Math.floor(Math.random() * chars.length));
    }

    // Try to detect office from URL or use 'office1' as default
    const urlParams = new URLSearchParams(window.location.search);
    const office = urlParams.get('office') || 'office1';

    return `${office}-${random}`;
  }

  /**
   * Get or create workstation ID
   */
  function getWorkstationId() {
    let id = localStorage.getItem(STORAGE_KEYS.WORKSTATION_ID);
    if (!id) {
      id = generateWorkstationId();
      localStorage.setItem(STORAGE_KEYS.WORKSTATION_ID, id);
    }
    return id;
  }

  /**
   * Update connection status indicator in the UI
   */
  function updateStatusIndicator(status) {
    connectionStatus = status;
    const indicator = document.getElementById('contact-sync-status');
    if (indicator) {
      const config = STATUS_INDICATORS[status];
      indicator.style.backgroundColor = config.color;
      indicator.title = config.label;
    }
  }

  /**
   * Fetch contacts from the API or load from fallback
   */
  async function fetchContactsFromApi(since = null) {
    if (useFallback) {
      return loadLocalContacts();
    }

    try {
      updateStatusIndicator('syncing');

      let url = `${apiUrl}`;
      if (since) {
        url += `?since=${encodeURIComponent(since.toISOString())}`;
      }

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        console.warn('Contact API returned:', response.status);
        useFallback = true;
        return loadLocalContacts();
      }

      const result = await response.json();

      if (result.success && Array.isArray(result.data)) {
        // Store locally for fallback
        saveLocalContacts(result.data);
        lastSync = new Date(result.timestamp);
        localStorage.setItem(STORAGE_KEYS.LAST_SYNC, lastSync.toISOString());

        updateStatusIndicator('online');
        return result.data;
      }

      return [];
    } catch (error) {
      console.error('Failed to fetch contacts from API:', error);
      useFallback = true;
      updateStatusIndicator('offline');
      return loadLocalContacts();
    }
  }

  /**
   * Load contacts from localStorage (fallback mode)
   */
  function loadLocalContacts() {
    const stored = localStorage.getItem(STORAGE_KEYS.LOCAL_CONTACTS);
    return stored ? JSON.parse(stored) : [];
  }

  /**
   * Save contacts to localStorage
   */
  function saveLocalContacts(contactList) {
    localStorage.setItem(STORAGE_KEYS.LOCAL_CONTACTS, JSON.stringify(contactList));
  }

  /**
   * Add a contact to the local storage (for offline mode)
   */
  function addLocalContact(entry) {
    const localContacts = loadLocalContacts();
    entry.id = Date.now(); // Simple local ID
    entry.created_at = new Date().toISOString();
    entry.is_undone = 0;
    localContacts.push(entry);
    saveLocalContacts(localContacts);
    return entry;
  }

  /**
   * Rebuild contacts map from array
   */
  function rebuildContactsMap(contactList) {
    contacts.clear();
    for (const entry of contactList) {
      if (!contacts.has(entry.row_id)) {
        contacts.set(entry.row_id, []);
      }
      contacts.get(entry.row_id).push(entry);
    }
  }

  /**
   * Log a new contact action to the API or localStorage
   */
  async function logContact(entry) {
    // Validate required fields
    if (!entry.row_id || !entry.contact_type || !entry.staff_name) {
      throw new Error('Missing required fields: row_id, contact_type, staff_name');
    }

    // Add workstation info
    entry.workstation = workstationId;
    entry.contact_status = entry.contact_status || 'completed';

    if (useFallback) {
      return addLocalContact(entry);
    }

    try {
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(entry)
      });

      if (!response.ok) {
        console.warn('Failed to log contact, falling back to local storage');
        useFallback = true;
        return addLocalContact(entry);
      }

      const result = await response.json();
      if (result.success && result.data) {
        // Add to local cache
        if (!contacts.has(entry.row_id)) {
          contacts.set(entry.row_id, []);
        }
        contacts.get(entry.row_id).push(result.data);

        // Update local storage
        const localContacts = loadLocalContacts();
        localContacts.push(result.data);
        saveLocalContacts(localContacts);

        updateStatusIndicator('online');
        return result.data;
      }
    } catch (error) {
      console.error('Error logging contact:', error);
      useFallback = true;
      return addLocalContact(entry);
    }
  }

  /**
   * Undo a contact log entry
   */
  async function undoContact(logId) {
    if (useFallback) {
      console.warn('Cannot undo in offline mode');
      return false;
    }

    try {
      const url = `${apiUrl}/${logId}/undo`;
      const response = await fetch(url, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        console.error('Failed to undo contact:', response.status);
        return false;
      }

      const result = await response.json();

      if (result.success) {
        // Refresh contacts
        await fetchAndUpdateContacts(lastSync);
        return true;
      }
    } catch (error) {
      console.error('Error undoing contact:', error);
    }

    return false;
  }

  /**
   * Check if a row has been contacted by anyone
   */
  function isContacted(rowId) {
    return contacts.has(rowId) && contacts.get(rowId).length > 0;
  }

  /**
   * Get contact info for a specific row
   */
  function getContactInfo(rowId) {
    const entries = contacts.get(rowId);
    if (!entries || entries.length === 0) {
      return null;
    }

    // Return most recent contact
    const latest = entries[entries.length - 1];
    return {
      contact_type: latest.contact_type,
      contact_name: latest.contact_name,
      staff_name: latest.staff_name,
      created_at: latest.created_at,
      workstation: latest.workstation
    };
  }

  /**
   * Fetch and update contacts internally
   */
  async function fetchAndUpdateContacts(since = null) {
    const contactList = await fetchContactsFromApi(since);
    rebuildContactsMap(contactList);

    if (onUpdateCallback && typeof onUpdateCallback === 'function') {
      onUpdateCallback(contactList);
    }

    return contactList;
  }

  /**
   * Start polling for contact updates
   */
  function startPolling() {
    if (pollingHandle) {
      console.warn('Polling already active');
      return;
    }

    pollingHandle = setInterval(async () => {
      await fetchAndUpdateContacts(lastSync);
    }, pollInterval);

    console.log(`Contact polling started (interval: ${pollInterval}ms)`);
  }

  /**
   * Stop polling for contact updates
   */
  function stopPolling() {
    if (pollingHandle) {
      clearInterval(pollingHandle);
      pollingHandle = null;
      console.log('Contact polling stopped');
    }
  }

  /**
   * Initialize the contact sync module
   */
  async function init(url, onUpdateCb = null) {
    apiUrl = url;
    onUpdateCallback = onUpdateCb;
    workstationId = getWorkstationId();

    console.log(`Contact Sync initialized. Workstation: ${workstationId}`);

    // Create status indicator in DOM if not already present
    if (!document.getElementById('contact-sync-status')) {
      const indicator = document.createElement('div');
      indicator.id = 'contact-sync-status';
      indicator.style.cssText = `
        position: fixed;
        bottom: 20px;
        right: 20px;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background-color: #ef4444;
        cursor: pointer;
        z-index: 10000;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
      `;
      indicator.title = 'Contact Sync Status';
      document.body.appendChild(indicator);
    }

    // Initial fetch
    await fetchAndUpdateContacts();

    // Start polling
    startPolling();
  }

  /**
   * Get current status
   */
  function getStatus() {
    return {
      workstationId,
      connectionStatus,
      useFallback,
      lastSync,
      contactCount: contacts.size,
      totalEntries: Array.from(contacts.values()).reduce((sum, arr) => sum + arr.length, 0)
    };
  }

  // Public API
  return {
    init,
    fetchContacts: fetchAndUpdateContacts,
    logContact,
    undoContact,
    isContacted,
    getContactInfo,
    startPolling,
    stopPolling,
    generateWorkstationId,
    getStatus,
    // For testing/debugging
    _getWorkstationId: () => workstationId,
    _setApiUrl: (url) => { apiUrl = url; },
    _getConnectionStatus: () => connectionStatus
  };
})();

// Export for use in browsers and modules
if (typeof module !== 'undefined' && module.exports) {
  module.exports = ContactSync;
}

getAuthedUser().then((user) => {
  /*
    Any component that needs user info, or the lack thereof, should listen for
    this event. Example: 
    document.addEventListener("afterAuthCheck", (e) => {
      const { user } = e.detail;
      console.log(user); // { did: "1234", handle: "gooddog.bsky.social" }  
    })
  */
  document.dispatchEvent(
    new CustomEvent("afterAuthCheck", { detail: { user } })
  );
});

/**
 * @returns {Promise<{did: string, handle: string}|null>} The authenticated user, or null if not authenticated.
 */
async function getAuthedUser() {
  try {
    const res = await fetch(`${API_BASE}/oauth/me`, FETCH_OPTS);
    if (res.ok) {
      const user = await res.json();
      return user;
    }
  } catch (e) {
    return null;
  }

  return null;
}

/**
 * @param {string} handle
 * @returns {Promise<{did: string, handle: string}|null>} The resolved user, or null if not found.
 */
async function lookupHandle(handle) {
  try {
    const res = await fetch(
      `${API_BASE}/api/resolve/${encodeURIComponent(handle)}`,
      FETCH_OPTS
    );

    if (!res.ok) throw new Error("Could not resolve handle");

    const user = await res.json();
    return user;
  } catch (err) {
    return null;
  }
}

/**
 * @param {string} [did]
 * @returns {Promise<{data: Array|null, error: string|null}>}
 */
async function getActivities(did) {
  const url = `${API_BASE}/api/activities${
    did ? `/${encodeURIComponent(did)}` : ""
  }`;

  try {
    const res = await fetch(url, FETCH_OPTS);
    if (!res.ok)
      return {
        data: null,
        error: `HTTP ${res.status}`,
      };
    const data = await res.json();
    return {
      data,
      error: null,
    };
  } catch (err) {
    return {
      data: null,
      error: err.message,
    };
  }
}

/**
 * @param {string} did
 * @param {string} rkey
 * @returns {Promise<{data: Object|null, error: string|null}>}
 */
async function getActivity(did, rkey) {
  const url = `${API_BASE}/api/activities/${encodeURIComponent(
    did
  )}/${encodeURIComponent(rkey)}`;

  try {
    const res = await fetch(url, FETCH_OPTS);
    if (!res.ok)
      return {
        data: null,
        error: `HTTP ${res.status}`,
      };
    const data = await res.json();
    return {
      data,
      error: null,
    };
  } catch (err) {
    return {
      data: null,
      error: err.message,
    };
  }
}

/**
 * @param {string} type
 * @returns {string}
 */
function getActivityTypeDisplayName(type) {
  let displayName;

  switch (type) {
    case "cycling":
      displayName = "ride";
      break;
    case "walking":
      displayName = "walk";
      break;
    case "hiking":
      displayName = "hike";
      break;
    default:
      displayName = type;
  }

  return toTitleCase(displayName);
}

/**
 * @param {string} iso
 * @returns {string}
 */
function getTimeOfDay(iso) {
  const hour = new Date(iso).getHours();
  if (hour < 12) return "Morning";
  if (hour < 17) return "Afternoon";
  return "Evening";
}

function metersToMiles(m) {
  return (parseFloat(m) / 1609.344).toFixed(1);
}

function msToMph(ms) {
  return (parseFloat(ms) * 2.23694).toFixed(1);
}

function metersToFeet(m) {
  return Math.round(parseFloat(m) * 3.28084);
}

/**
 * @param {string} str
 * @returns {string}
 */
function toTitleCase(str) {
  return str.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) {
    return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  }
  return `${m}:${String(s).padStart(2, "0")}`;
}

/**
 * Decode a Google encoded polyline string into an array of [lng, lat] pairs.
 * @param {string} encoded
 * @returns {Array<[number, number]>}
 */
function decodePolyline(encoded) {
  const coords = [];
  let i = 0;
  let lat = 0;
  let lng = 0;

  while (i < encoded.length) {
    let shift = 0;
    let result = 0;
    let byte;

    do {
      byte = encoded.charCodeAt(i++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20);

    lat += result & 1 ? ~(result >> 1) : result >> 1;

    shift = 0;
    result = 0;

    do {
      byte = encoded.charCodeAt(i++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20);

    lng += result & 1 ? ~(result >> 1) : result >> 1;

    coords.push([lng / 1e5, lat / 1e5]);
  }

  return coords;
}

function formatDate(iso) {
  return new Date(iso).toLocaleDateString("en-US", {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

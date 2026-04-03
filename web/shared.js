const API_BASE = "http://127.0.0.1:8000";

const fetchOpts = { credentials: "include" };

let currentUser = null;

function metersToMiles(m) {
  return (parseFloat(m) / 1609.344).toFixed(1);
}

function msToMph(ms) {
  return (parseFloat(ms) * 2.23694).toFixed(1);
}

function metersToFeet(m) {
  return Math.round(parseFloat(m) * 3.28084);
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

function formatDate(iso) {
  return new Date(iso).toLocaleDateString("en-US", {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function renderActivity(activity, showDelete) {
  const stats = [];

  stats.push({
    label: "Distance",
    value: `${metersToMiles(activity.distance)} mi`,
  });
  stats.push({
    label: "Moving Time",
    value: formatDuration(activity.moving_time),
  });
  stats.push({
    label: "Elapsed Time",
    value: formatDuration(activity.elapsed_time),
  });

  if (activity.elevation_gain) {
    stats.push({
      label: "Elevation",
      value: `${metersToFeet(activity.elevation_gain)} ft`,
    });
  }
  if (activity.avg_speed) {
    stats.push({
      label: "Avg Speed",
      value: `${msToMph(activity.avg_speed)} mph`,
    });
  }
  if (activity.avg_heart_rate) {
    const hr = activity.max_heart_rate
      ? `${activity.avg_heart_rate} / ${activity.max_heart_rate}`
      : `${activity.avg_heart_rate}`;
    stats.push({ label: "Heart Rate", value: `${hr} bpm` });
  }
  if (activity.avg_power) {
    stats.push({ label: "Avg Power", value: `${activity.avg_power} W` });
  }

  const statsHtml = stats
    .map(
      (s) => `<div class="stat">
        <div class="stat-label">${s.label}</div>
        <div class="stat-value">${s.value}</div>
      </div>`
    )
    .join("");

  const device = activity.device
    ? `<div class="activity-device">${activity.device}</div>`
    : "";

  const deleteBtn = showDelete
    ? `<form class="delete-form" data-did="${activity.did}" data-rkey="${activity.rkey}">
        <button type="submit" class="delete-btn">Delete</button>
      </form>`
    : "";

  return `<article class="activity" id="activity-${activity.rkey}">
    <div class="activity-header">
      <div class="activity-title">${activity.title || "Untitled Ride"}</div>
      <div class="activity-did">${activity.did}</div>
    </div>
    <div class="activity-date">${formatDate(activity.started_at)}</div>
    <div class="activity-stats">${statsHtml}</div>
    ${device}
    ${deleteBtn}
  </article>`;
}

async function loadActivities(container, did, showDelete) {
  container.innerHTML = '<p class="loading">Loading activities\u2026</p>';

  let url = `${API_BASE}/api/activities`;
  if (did) {
    url = `${API_BASE}/api/activities/${encodeURIComponent(did)}`;
  }

  try {
    const res = await fetch(url, fetchOpts);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const activities = await res.json();

    if (activities.length === 0) {
      container.innerHTML = "<p>No activities found.</p>";
      return;
    }

    container.innerHTML = activities
      .map((a) => renderActivity(a, showDelete))
      .join("");

    if (showDelete) {
      container.querySelectorAll(".delete-form").forEach((form) => {
        form.addEventListener("submit", handleDelete);
      });
    }
  } catch (err) {
    container.innerHTML = `<p class="error">${err.message}</p>`;
  }
}

async function handleDelete(e) {
  e.preventDefault();
  const form = e.target;
  const did = form.dataset.did;
  const rkey = form.dataset.rkey;
  const btn = form.querySelector("button");

  if (!confirm("Delete this activity?")) return;

  btn.disabled = true;
  btn.textContent = "Deleting\u2026";

  try {
    const res = await fetch(
      `${API_BASE}/api/activities/${did}/${rkey}/delete`,
      {
        ...fetchOpts,
        method: "POST",
      }
    );

    if (!res.ok) {
      const err = await res.json();
      alert(err.error || "Delete failed");
      btn.disabled = false;
      btn.textContent = "Delete";
      return;
    }

    const article = document.getElementById(`activity-${rkey}`);
    if (article) article.remove();
  } catch (err) {
    alert("Delete failed: " + err.message);
    btn.disabled = false;
    btn.textContent = "Delete";
  }
}

// --- Auth ---

function renderLoginForm() {
  return `<form id="login-form">
    <input type="text" id="login-handle" placeholder="Handle (e.g. alice.bsky.social)" />
    <button type="submit">Sign in</button>
  </form>`;
}

function renderLoggedIn(handle) {
  return `<a href="/profile/?handle=${handle}" class="auth-handle">${handle}</a>
    <button id="logout-btn" type="button">Sign out</button>`;
}

async function checkAuth() {
  const container = document.getElementById("auth");
  try {
    const res = await fetch(`${API_BASE}/oauth/me`, fetchOpts);
    if (res.ok) {
      currentUser = await res.json();
      container.innerHTML = renderLoggedIn(currentUser.handle);
      document.getElementById("logout-btn").addEventListener("click", logout);
      return;
    }
  } catch (e) {
    // Not logged in
  }
  currentUser = null;
  container.innerHTML = renderLoginForm();
  document.getElementById("login-form").addEventListener("submit", login);
}

async function login(e) {
  e.preventDefault();
  const input = document.getElementById("login-handle");
  const username = input.value.trim();
  if (!username) return;

  const btn = e.target.querySelector("button");
  btn.disabled = true;
  btn.textContent = "Signing in\u2026";

  try {
    const res = await fetch(`${API_BASE}/oauth/login`, {
      ...fetchOpts,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username }),
    });

    if (!res.ok) {
      const err = await res.json();
      alert(err.error || "Login failed");
      return;
    }

    const data = await res.json();
    window.location.href = data.redirect_url;
  } catch (err) {
    alert("Login failed: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Sign in";
  }
}

async function logout() {
  try {
    await fetch(`${API_BASE}/oauth/logout`, {
      ...fetchOpts,
      method: "POST",
    });
  } catch (e) {
    // Continue with local cleanup even if server call fails
  }
  currentUser = null;
  window.location.href = "/";
}

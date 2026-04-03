const API_BASE = "http://localhost:8000";

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

function renderActivity(activity) {
  const stats = [];

  stats.push({ label: "Distance", value: `${metersToMiles(activity.distance)} mi` });
  stats.push({ label: "Moving Time", value: formatDuration(activity.moving_time) });
  stats.push({ label: "Elapsed Time", value: formatDuration(activity.elapsed_time) });

  if (activity.elevation_gain) {
    stats.push({ label: "Elevation", value: `${metersToFeet(activity.elevation_gain)} ft` });
  }
  if (activity.avg_speed) {
    stats.push({ label: "Avg Speed", value: `${msToMph(activity.avg_speed)} mph` });
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

  return `<article class="activity">
    <div class="activity-header">
      <div class="activity-title">${activity.title || "Untitled Ride"}</div>
      <div class="activity-did">${activity.did}</div>
    </div>
    <div class="activity-date">${formatDate(activity.started_at)}</div>
    <div class="activity-stats">${statsHtml}</div>
    ${device}
  </article>`;
}

async function loadActivities(did) {
  const container = document.getElementById("activities");
  container.innerHTML = '<p class="loading">Loading activities\u2026</p>';

  let url = `${API_BASE}/api/activities`;
  if (did) {
    url = `${API_BASE}/api/activities/${encodeURIComponent(did)}`;
  }

  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const activities = await res.json();

    if (activities.length === 0) {
      container.innerHTML = "<p>No activities found.</p>";
      return;
    }

    container.innerHTML = activities.map(renderActivity).join("");
  } catch (err) {
    container.innerHTML = `<p class="error">${err.message}</p>`;
  }
}

const form = document.getElementById("lookup");
const input = document.getElementById("handle");

form.addEventListener("submit", (e) => {
  e.preventDefault();
  const did = input.value.trim();
  loadActivities(did || null);
});

// Load global feed on page load
loadActivities();

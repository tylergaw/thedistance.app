const params = new URLSearchParams(window.location.search);
const handle = params.get("handle");

async function init() {
  await checkAuth();

  if (!handle) {
    document.getElementById("activities").innerHTML =
      '<p class="error">No handle provided.</p>';
    return;
  }

  document.getElementById("profile-handle").textContent = handle;

  let did;
  try {
    const res = await fetch(
      `${API_BASE}/api/resolve/${encodeURIComponent(handle)}`,
      fetchOpts
    );
    if (!res.ok) throw new Error("Could not resolve handle");
    const data = await res.json();
    did = data.did;
  } catch (err) {
    document.getElementById("activities").innerHTML =
      `<p class="error">${err.message}</p>`;
    return;
  }

  const isOwner = currentUser && currentUser.did === did;
  loadActivities(document.getElementById("activities"), did, isOwner);
}

init();

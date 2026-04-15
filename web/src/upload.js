let parsedActivities = [];

async function init() {
  await checkAuth();

  if (!currentUser) {
    document.getElementById("upload-form").style.display = "none";
    document.getElementById("upload-error").innerHTML =
      '<p class="error">You must be signed in to upload activities.</p>';
    return;
  }

  document
    .getElementById("upload-form")
    .addEventListener("submit", handleUpload);
}

async function handleUpload(e) {
  e.preventDefault();

  const fileInput = document.getElementById("file-input");
  const files = fileInput.files;

  if (files.length === 0) return;

  const errorContainer = document.getElementById("upload-error");
  const previewContainer = document.getElementById("preview");
  const btn = e.target.querySelector("button");

  errorContainer.innerHTML = "";
  previewContainer.innerHTML = '<p class="loading">Parsing files\u2026</p>';
  btn.disabled = true;

  const formData = new FormData();
  for (const file of files) {
    formData.append("files", file);
  }

  try {
    const res = await fetch(`${API_BASE}/api/parse`, {
      ...FETCH_OPTS,
      method: "POST",
      body: formData,
    });

    if (!res.ok) {
      const err = await res.json();
      errorContainer.innerHTML = `<p class="error">${
        err.error || "Upload failed"
      }</p>`;
      previewContainer.innerHTML = "";
      return;
    }

    const data = await res.json();
    const activities = data.activities;

    if (activities.length === 0) {
      previewContainer.innerHTML =
        "<p>No activities found in uploaded files.</p>";
      return;
    }

    parsedActivities = activities;

    previewContainer.innerHTML =
      "<h3>Preview</h3>" +
      activities.map((a) => renderActivity(a, false)).join("") +
      '<button id="create-btn" type="button">Create Records</button>';

    document
      .getElementById("create-btn")
      .addEventListener("click", handleCreate);
  } catch (err) {
    errorContainer.innerHTML = `<p class="error">${err.message}</p>`;
    previewContainer.innerHTML = "";
  } finally {
    btn.disabled = false;
  }
}

async function handleCreate() {
  const btn = document.getElementById("create-btn");
  const errorContainer = document.getElementById("upload-error");

  btn.disabled = true;
  btn.textContent = "Creating\u2026";
  errorContainer.innerHTML = "";

  const errors = [];

  for (const activity of parsedActivities) {
    try {
      const res = await fetch(`${API_BASE}/api/activities`, {
        ...FETCH_OPTS,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(activity),
      });

      if (!res.ok) {
        try {
          const err = await res.json();
          errors.push(err.error || "Create failed");
        } catch {
          errors.push(`Create failed (HTTP ${res.status})`);
        }
      }
    } catch (err) {
      errors.push(err.message);
    }
  }

  if (errors.length > 0) {
    errorContainer.innerHTML = errors
      .map((e) => `<p class="error">${e}</p>`)
      .join("");
    btn.disabled = false;
    btn.textContent = "Create Records";
    return;
  }

  window.location.href = `/profile/${currentUser.handle}`;
}

init();

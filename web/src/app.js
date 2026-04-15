/**
 * @returns {Promise<{did: string, handle: string}|null>} The authenticated user, or null if not authenticated.
 */
async function getAuthedUser() {
  try {
    const res = await fetch(`${API_BASE}/oauth/me`, FETCH_OPTS);
    if (res.ok) {
      user = await res.json();
      return user;
    }
  } catch (e) {
    return null;
  }

  return null;
}

getAuthedUser().then((user) => {
  if (user) {
    if (typeof renderPageHeader === "function") {
      renderPageHeader({ user });
    }
  }
});

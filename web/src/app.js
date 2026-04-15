getAuthedUser().then((user) => {
  if (user) {
    try {
      if (isFunction(renderPageHeader)) {
        renderPageHeader({ user });
      }

      if (isFunction(renderHomeAuth)) {
        renderHomeAuth({ user });
      }
    } catch (e) {
      console.warn(e);
    }
  }
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

function isFunction(func) {
  return typeof func === "function";
}

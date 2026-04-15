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

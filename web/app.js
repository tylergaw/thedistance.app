const form = document.getElementById("lookup");
const input = document.getElementById("handle");
const container = document.getElementById("activities");

form.addEventListener("submit", (e) => {
  e.preventDefault();
  const did = input.value.trim();
  loadActivities(container, did || null, false);
});

checkAuth();
loadActivities(container, null, false);

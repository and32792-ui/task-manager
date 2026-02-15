const form = document.getElementById("task-form");
const input = document.getElementById("task-input");
const list = document.getElementById("task-list");

async function fetchTasks() {
  const res = await fetch("/api/tasks");
  const tasks = await res.json();
  renderTasks(tasks);
}

function renderTasks(tasks) {
  list.innerHTML = "";
  tasks.forEach((task) => {
    const li = document.createElement("li");
    li.className = "task-item" + (task.done ? " done" : "");

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = task.done;
    checkbox.addEventListener("change", () => toggleTask(task.id, !task.done));

    const span = document.createElement("span");
    span.textContent = task.title;

    const deleteBtn = document.createElement("button");
    deleteBtn.className = "delete-btn";
    deleteBtn.textContent = "\u00d7";
    deleteBtn.addEventListener("click", () => deleteTask(task.id));

    li.append(checkbox, span, deleteBtn);
    list.appendChild(li);
  });
}

async function addTask(title) {
  await fetch("/api/tasks", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title }),
  });
  fetchTasks();
}

async function toggleTask(id, done) {
  await fetch(`/api/tasks/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ done }),
  });
  fetchTasks();
}

async function deleteTask(id) {
  await fetch(`/api/tasks/${id}`, { method: "DELETE" });
  fetchTasks();
}

form.addEventListener("submit", (e) => {
  e.preventDefault();
  const title = input.value.trim();
  if (!title) return;
  input.value = "";
  addTask(title);
});

fetchTasks();
